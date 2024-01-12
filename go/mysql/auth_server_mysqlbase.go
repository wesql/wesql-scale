/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package mysql

import (
	"context"
	"crypto/subtle"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/internal/global"
	"vitess.io/vitess/go/vt/servenv"

	"golang.org/x/exp/slices"

	stringutil "vitess.io/vitess/go/mysql/utils"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

var (
	mysqlAuthServerMysqlBaseReloadInterval time.Duration
)

const (
	defaultMysqlAuthServerMysqlBaseReloadInterval = 30 * time.Second
)
const (
	PluginCachingSha2Password = "caching_sha2_password"
	PluginMysqlNativePassword = "mysql_native_password"
)

// AuthServerMysqlBase implements AuthServer using a static configuration.
type AuthServerMysqlBase struct {
	methods []AuthMethod
	// This mutex helps us prevent data races between the multiple updates of entries.
	mu sync.Mutex
	// This mutex helps us prevent data races between the multiple updates of cacheEntries.
	cacheLatch     sync.Mutex
	reloadInterval time.Duration
	qs             queryservice.QueryService
	// entries contains the users, passwords and user data.
	entries map[string][]*AuthServerMysqlBaseEntry
	//cacheEntries used by fast-authentication
	cacheEntries map[string][]*AuthServerMysqlBaseEntry
	sigChan      chan os.Signal
	ticker       *time.Ticker
	skipPassword bool
}

func init() {
	servenv.OnParseFor("vtgate", func(fs *pflag.FlagSet) {
		fs.DurationVar(&mysqlAuthServerMysqlBaseReloadInterval, "mysql_auth_mysqlbased_reload_interval", defaultMysqlAuthServerMysqlBaseReloadInterval, "Ticker to reload credentials")
	})
}

var instance *AuthServerMysqlBase
var once sync.Once

func GetAuthServerMysqlBase() *AuthServerMysqlBase {
	once.Do(func() {
		instance = NewAuthServerMysqlBase()
	})
	return instance
}

func ReloadUsers() (uint64, error) {
	return GetAuthServerMysqlBase().reLoadUser()
}

// AuthServerMysqlBaseEntry stores the values for a given user.
type AuthServerMysqlBaseEntry struct {
	MysqlCachingSha2Password string
	// MysqlNativePassword is generated by password hashing methods in MySQL.
	// These changes are illustrated by changes in the result from the PASSWORD() function
	// that computes password hash values and in the structure of the user table where passwords are stored.
	// mysql> SELECT PASSWORD('mypass');
	// +-------------------------------------------+
	// | PASSWORD('mypass')                        |
	// +-------------------------------------------+
	// | *6C8989366EAF75BB670AD8EA7A7FC1176A95CEF4 |
	// +-------------------------------------------+
	// MysqlNativePassword's format looks like "*6C8989366EAF75BB670AD8EA7A7FC1176A95CEF4", it store a hashing value.
	// Use MysqlNativePassword in auth config, maybe more secure. After all, it is cryptographic storage.
	MysqlNativePassword string
	ScramblePassword    []byte
	Password            string
	UserData            string
	plugin              string
	SourceHost          string
	Groups              []string
	// patChars is compiled from Host, cached for pattern match performance.
	patChars []byte
	patTypes []byte
}

func NewAuthServerMysqlBase() *AuthServerMysqlBase {
	a := &AuthServerMysqlBase{
		entries:        make(map[string][]*AuthServerMysqlBaseEntry),
		cacheEntries:   make(map[string][]*AuthServerMysqlBaseEntry),
		reloadInterval: mysqlAuthServerMysqlBaseReloadInterval,
	}
	a.methods = []AuthMethod{NewMysqlNativeAuthMethod(a, a)}
	a.methods = append(a.methods, NewSha2CachingAuthMethod(a, a, a))

	RegisterAuthServer(global.AuthServerMysqlBased, a)
	a.installSignalHandlers()
	return a
}

func (a *AuthServerMysqlBase) installSignalHandlers() {
	a.sigChan = make(chan os.Signal, 1)
	signal.Notify(a.sigChan, syscall.SIGHUP)
	go func() {
		for range a.sigChan {
			a.reLoadUser()
		}
	}()

	// If duration is set, it will reload configuration every interval
	if a.reloadInterval > 0 {
		a.ticker = time.NewTicker(a.reloadInterval)
		go func() {
			for range a.ticker.C {
				a.sigChan <- syscall.SIGHUP
			}
		}()
	}
}

func (a *AuthServerMysqlBase) deleteUserFromCache(user string) {
	a.cacheLatch.Lock()
	defer a.cacheLatch.Unlock()
	delete(a.cacheEntries, user)
}
func (a *AuthServerMysqlBase) addUserToCache(user string, entry *AuthServerMysqlBaseEntry) {
	a.cacheLatch.Lock()
	defer a.cacheLatch.Unlock()
	a.cacheEntries[user] = append(a.cacheEntries[user], entry)

}

// isCacheExistsInEntry use to remove entry from cache
func isCacheExistsInEntry(user string, host string, list []*AuthServerMysqlBaseEntry) bool {
	for _, entry := range list {
		if entry.UserData == user && entry.SourceHost == host {
			return true
		}
	}
	return false
}

// reLoadUser load user information from mysql.user and update entries and cacheEntries
func (a *AuthServerMysqlBase) reLoadUser() (uint64, error) {
	if a.qs == nil {
		return 0, fmt.Errorf("mysql_auth_server_impl is not mysqlbased")
	}
	ctx := context.Background()
	target := &querypb.Target{
		Keyspace:   global.DefaultKeyspace,
		Shard:      global.DefaultShard,
		TabletType: topodata.TabletType_PRIMARY,
	}
	// pull user from mysql.user
	qr, err := a.qs.ExecuteInternal(ctx, target, FetchUser, nil, 0, 0, nil)
	if err != nil {
		return 0, err
	}
	affectedNum := len(qr.Rows)
	entries := make(map[string][]*AuthServerMysqlBaseEntry)
	for _, rows := range qr.Rows {
		user := rows[0].ToString()
		host := rows[1].ToString()
		plugin := rows[2].ToString()
		authenticationString := rows[3].ToString()
		entrie := AuthServerMysqlBaseEntry{
			plugin:     plugin,
			UserData:   user,
			SourceHost: host,
		}
		entrie.patChars, entrie.patTypes = stringutil.CompilePatternBytes(host, '\\')
		if plugin == PluginCachingSha2Password {
			entrie.MysqlCachingSha2Password = authenticationString
		}
		if plugin == PluginMysqlNativePassword {
			entrie.MysqlNativePassword = authenticationString
		}
		entries[user] = append(entries[user], &entrie)
	}
	// sort by host
	for _, list := range entries {
		slices.SortFunc(list, compareBaseRecord)
	}
	// remove user who had been deleted from cacheEntries
	a.cacheLatch.Lock()
	defer a.cacheLatch.Unlock()
	for key := range a.cacheEntries {
		list, ok := entries[key]
		if !ok {
			delete(a.cacheEntries, key)
		} else {
			newEntries := make([]*AuthServerMysqlBaseEntry, 0)
			// Only cache users who are listed in the mysql.user table
			for _, entry := range a.cacheEntries[key] {
				if isCacheExistsInEntry(entry.UserData, entry.SourceHost, list) {
					newEntries = append(newEntries, entry)
				}
			}
			// sort by host
			slices.SortFunc(newEntries, compareBaseRecord)
			a.cacheEntries[key] = newEntries
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries = entries
	return uint64(affectedNum), nil
}

// SetQueryService set QueryService
func (a *AuthServerMysqlBase) SetQueryService(conn queryservice.QueryService) {
	a.qs = conn
	_, err := a.reLoadUser()
	if err != nil {
		log.Println("reload fail")
	}
}

// AuthMethods returns the AuthMethod instances this auth server can handle.
func (a *AuthServerMysqlBase) AuthMethods() []AuthMethod {
	return a.methods
}

// HandleUser is part of the Validator interface.
func (a *AuthServerMysqlBase) HandleUser(user string, plugin string) bool {
	if a.entries[user] == nil {
		return false
	}
	for _, entry := range a.entries[user] {
		if entry.plugin == plugin {
			return true
		}
	}
	return false
}

// DefaultAuthMethodDescription returns the default auth method in the handshake which
// is CachingSha2Password for this auth server.
func (a *AuthServerMysqlBase) DefaultAuthMethodDescription() AuthMethodDescription {
	return CachingSha2Password
}

// UserEntryWithCacheHash implements password lookup based on a
// caching_sha2_password fast authentication hash that is negotiated with the client.
func (a *AuthServerMysqlBase) UserEntryWithCacheHash(_ *Conn, salt []byte, user string, authResponse []byte, remoteAddr net.Addr) (Getter, CacheState, error) {
	a.cacheLatch.Lock()
	entries, ok := a.cacheEntries[user]
	a.cacheLatch.Unlock()
	if !ok {
		return &StaticUserData{}, AuthNeedMoreData, nil
	}
	// Validate the password.
	for _, entry := range entries {
		isPass := VerifyHashedCachingSha2Password(authResponse, salt, entry.ScramblePassword)
		if MatchHost(remoteAddr, entry) && isPass {
			return &StaticUserData{entry.UserData, entry.SourceHost, entry.Groups}, AuthAccepted, nil
		}
	}
	return &StaticUserData{}, AuthRejected, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
}

// UserEntryWithFullAuth implements password lookup based on a
// caching_sha2_password full authentication hash that is negotiated with the client.
func (a *AuthServerMysqlBase) UserEntryWithFullAuth(_ *Conn, _ []byte, user string, password string, remoteAddr net.Addr) (Getter, error) {
	a.mu.Lock()
	entries, ok := a.entries[user]
	a.mu.Unlock()
	if !ok {
		return &StaticUserData{}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	for _, entry := range entries {
		// Validate the password.
		if entry.MysqlCachingSha2Password != "" {
			pwhash, _ := ScrambleSha2Password(password, []byte(entry.MysqlCachingSha2Password))
			if MatchHost(remoteAddr, entry) && subtle.ConstantTimeCompare([]byte(pwhash), []byte(entry.MysqlCachingSha2Password)) == 1 {
				entry.ScramblePassword = ScramblePassword([]byte(password))
				a.addUserToCache(user, entry)
				return &StaticUserData{entry.UserData, entry.SourceHost, entry.Groups}, nil
			}
		} else {
			// Validate the host.
			if MatchHost(remoteAddr, entry) {
				return &StaticUserData{entry.UserData, entry.SourceHost, entry.Groups}, nil
			}
		}
	}
	return &StaticUserData{}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
}

// UserEntryWithHash implements password lookup based on a
// mysql_native_password hash that is negotiated with the client.
func (a *AuthServerMysqlBase) UserEntryWithHash(_ *Conn, salt []byte, user string, authResponse []byte, remoteAddr net.Addr) (Getter, error) {
	a.mu.Lock()
	entries, ok := a.entries[user]
	a.mu.Unlock()

	if !ok {
		return &StaticUserData{}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	for _, entry := range entries {
		if entry.MysqlNativePassword != "" {
			hash, err := DecodeMysqlNativePasswordHex(entry.MysqlNativePassword)
			if err != nil {
				return &StaticUserData{entry.UserData, entry.SourceHost, entry.Groups}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
			}
			isPass := VerifyHashedMysqlNativePassword(authResponse, salt, hash)
			if MatchHost(remoteAddr, entry) && isPass {
				return &StaticUserData{entry.UserData, entry.SourceHost, entry.Groups}, nil
			}
		} else {
			// Validate the host.
			if MatchHost(remoteAddr, entry) {
				return &StaticUserData{entry.UserData, entry.SourceHost, entry.Groups}, nil
			}
		}
	}
	return &StaticUserData{}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
}

func compareBaseRecord(x, y *AuthServerMysqlBaseEntry) bool {
	// Compare two item by user's host first.
	c1 := compareHost(x.SourceHost, y.SourceHost)
	if c1 < 0 {
		return true
	}
	if c1 > 0 {
		return false
	}

	// Then, compare item by user's name value.
	return x.UserData < y.UserData
}

// compareHost compares two host string using some special rules, return value 1, 0, -1 means > = <.
func compareHost(x, y string) int {
	// The more-specific, the smaller it is.
	// The pattern '%' means “any host” and is least specific.
	if y == `%` {
		if x == `%` {
			return 0
		}
		return -1
	}

	// The empty string '' also means “any host” but sorts after '%'.
	if y == "" {
		if x == "" {
			return 0
		}
		return -1
	}

	// One of them end with `%`.
	xEnd := strings.HasSuffix(x, `%`)
	yEnd := strings.HasSuffix(y, `%`)
	if xEnd || yEnd {
		switch {
		case !xEnd && yEnd:
			return -1
		case xEnd && !yEnd:
			return 1
		case xEnd && yEnd:
			// 192.168.199.% smaller than 192.168.%
			// A not very accurate comparison, compare them by length.
			if len(x) > len(y) {
				return -1
			}
		}
		return 0
	}

	// For other case, the order is nondeterministic.
	switch x < y {
	case true:
		return -1
	case false:
		return 1
	}
	return 0
}

func MatchHost(remoteAddr net.Addr, entry *AuthServerMysqlBaseEntry) bool {
	if entry.SourceHost == "" {
		return true
	}
	switch remoteAddr.(type) {
	case *net.UnixAddr:
		if entry.SourceHost == localhostName {
			return true
		}
	case *net.TCPAddr:
		if ExtractIPAddr(remoteAddr) == "::1" && entry.SourceHost == "localhost" {
			return true
		}
		// localhost match 127.0.0.1
		if ExtractIPAddr(remoteAddr) == "127.0.0.1" && entry.SourceHost == "localhost" {
			return true
		}
		if patternMatch(ExtractIPAddr(remoteAddr), entry.patChars, entry.patTypes) {
			return true
		}
	}
	return false
}

// patternMatch matches "%" the same way as ".*" in regular expression, for example,
// "10.0.%" would match "10.0.1" "10.0.1.118" ...
func patternMatch(str string, patChars, patTypes []byte) bool {
	return stringutil.DoMatchBytes(str, patChars, patTypes)
}

// NewHashPassword creates a new password for caching_sha2_password
func NewHashPassword(pwd string, pwhash string) string {
	pwHash, _ := ScrambleSha2Password(pwd, []byte(pwhash))
	return pwHash
}

name: {{.Name}}
on:
#  pull_request:
  workflow_dispatch:
  push:
    branches:
      - main
concurrency:
  group: format('{0}-{1}', ${{"{{"}} github.ref {{"}}"}}, '{{.Name}}')
  cancel-in-progress: true

env:
  GITHUB_PR_HEAD_SHA: "${{`{{ github.event.pull_request.head.sha }}`}}"

jobs:
  build:
    name: Run endtoend tests on {{.Name}}
    runs-on: ubuntu-22.04

    steps:
    - name: Skip CI
      run: |
        if [[ "{{"${{contains( github.event.pull_request.labels.*.name, 'Skip CI')}}"}}" == "true" ]]; then
          echo "skipping CI due to the 'Skip CI' label"
          exit 1
        fi

    - name: Check if workflow needs to be skipped
      id: skip-workflow
      run: |
        skip='false'
        if [[ "{{"${{github.event.pull_request}}"}}" ==  "" ]] && [[ "{{"${{github.ref}}"}}" != "refs/heads/main" ]] && [[ ! "{{"${{github.ref}}"}}" =~ ^refs/heads/release-[0-9]+\.[0-9]$ ]] && [[ ! "{{"${{github.ref}}"}}" =~ "refs/tags/.*" ]]; then
          skip='true'
        fi
        echo Skip ${skip}
        echo "skip-workflow=${skip}" >> $GITHUB_OUTPUT

    - name: Check out code
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: actions/checkout@v3

    - name: Check for changes in relevant files
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: frouioui/paths-filter@main
      id: changes
      with:
        token: ''
        filters: |
          end_to_end:
            - 'go/**/*.go'
            - 'test.go'
            - 'Makefile'
            - 'build.env'
            - 'go.sum'
            - 'go.mod'
            - 'proto/*.proto'
            - 'tools/**'
            - 'config/**'
            - 'bootstrap.sh'
            - '.github/workflows/{{.FileName}}'
            {{- if or (contains .Name "onlineddl") (contains .Name "schemadiff") }}
            - 'go/test/endtoend/onlineddl/vrepl_suite/testdata'
            {{- end}}

    - name: Set up Go
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
      uses: actions/setup-go@v3
      with:
        go-version: 1.20.1

    - name: Set up python
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
      uses: actions/setup-python@v4

    - name: Tune the OS
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
      run: |
        # Limit local port range to not use ports that overlap with server side
        # ports that we listen on.
        sudo sysctl -w net.ipv4.ip_local_port_range="22768 65535"
        # Increase the asynchronous non-blocking I/O. More information at https://dev.mysql.com/doc/refman/5.7/en/innodb-parameters.html#sysvar_innodb_use_native_aio
        echo "fs.aio-max-nr = 1048576" | sudo tee -a /etc/sysctl.conf
        sudo sysctl -p /etc/sysctl.conf

    - name: Get dependencies
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
      run: |
        {{if .InstallXtraBackup}}

        # Setup Percona Server for MySQL 8.0
        sudo apt-get update
        sudo apt-get install -y lsb-release gnupg2 curl
        wget https://repo.percona.com/apt/percona-release_latest.$(lsb_release -sc)_all.deb
        sudo DEBIAN_FRONTEND="noninteractive" dpkg -i percona-release_latest.$(lsb_release -sc)_all.deb
        sudo percona-release setup ps80
        sudo apt-get update

        # Install everything else we need, and configure
        sudo apt-get install -y percona-server-server percona-server-client make unzip g++ etcd git wget eatmydata xz-utils libncurses5

        {{else}}

        # Get key to latest MySQL repo
        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29
        # Setup MySQL 8.0
        wget -c https://dev.mysql.com/get/mysql-apt-config_0.8.24-1_all.deb
        echo mysql-apt-config mysql-apt-config/select-server select mysql-8.0 | sudo debconf-set-selections
        sudo DEBIAN_FRONTEND="noninteractive" dpkg -i mysql-apt-config*
        sudo apt-get update
        # Install everything else we need, and configure
        sudo apt-get install -y mysql-server mysql-client make unzip g++ etcd curl git wget eatmydata xz-utils libncurses5

        {{end}}

        sudo service mysql stop
        sudo service etcd stop
        sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
        sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld
        go mod download

        # install JUnit report formatter
        go install github.com/vitessio/go-junit-report@HEAD

        {{if .InstallXtraBackup}}

        sudo apt-get install percona-xtrabackup-80 lz4

        {{end}}

    {{if .MakeTools}}

    - name: Installing zookeeper and consul
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
      run: |
          make tools

    {{end}}

    - name: Run cluster endtoend test
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true'
      timeout-minutes: 45
      run: |
        # We set the VTDATAROOT to the /tmp folder to reduce the file path of mysql.sock file
        # which musn't be more than 107 characters long.
        export VTDATAROOT="/tmp/"
        source build.env

        set -x

        {{if .LimitResourceUsage}}
        # Increase our open file descriptor limit as we could hit this
        ulimit -n 65536
        cat <<-EOF>>./config/mycnf/mysql80.cnf
        innodb_buffer_pool_dump_at_shutdown=OFF
        innodb_buffer_pool_in_core_file=OFF
        innodb_buffer_pool_load_at_startup=OFF
        innodb_buffer_pool_size=64M
        innodb_doublewrite=OFF
        innodb_flush_log_at_trx_commit=0
        innodb_flush_method=O_DIRECT
        innodb_numa_interleave=ON
        innodb_adaptive_hash_index=OFF
        sync_binlog=0
        sync_relay_log=0
        performance_schema=OFF
        slow-query-log=OFF
        EOF
        {{end}}

        # run the tests however you normally do, then produce a JUnit XML file
        # failpoint
        chmod 755 ./test/failpoint/failpoints.sh && source ./test/failpoint/failpoints.sh
        echo "GO_FAILPOINTS=$GO_FAILPOINTS" >> $GITHUB_OUTPUT
        make failpoint-enable
        eatmydata -- go run test.go -docker={{if .Docker}}true -flavor={{.Platform}}{{else}}false{{end}} -follow -shard {{.Shard}}{{if .PartialKeyspace}} -partial-keyspace=true {{end}} | tee -a output.txt | go-junit-report -set-exit-code > report.xml
        make failpoint-disable

    - name: Print test output and Record test result
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.end_to_end == 'true' && always()
      run: |

        # print test output
        cat output.txt

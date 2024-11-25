CREATE TABLE IF NOT EXISTS mysql.branch_schema
(
    `id`               bigint unsigned NOT NULL AUTO_INCREMENT,
    `name`             varchar(64)     NOT NULL,
    `database`         varchar(64)     NOT NULL,
    `table`           varchar(64)     NOT NULL,
    `create_table_sql` text           NOT NULL,
    `schema_type`            varchar(16)     NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY(`name`, `database`, `table`)
    ) ENGINE = InnoDB;
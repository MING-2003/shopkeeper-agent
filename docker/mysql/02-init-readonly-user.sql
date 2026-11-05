-- 数仓只读账号：即使前两层防护被击穿，数据库层面也无法写入
CREATE USER IF NOT EXISTS 'dw_ro'@'%' IDENTIFIED BY 'dw_ro_pass';
GRANT SELECT ON dw.* TO 'dw_ro'@'%';
FLUSH PRIVILEGES;

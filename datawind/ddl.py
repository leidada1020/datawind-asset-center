# -*- coding: utf-8 -*-
"""建表 DDL（与 datawind_asset_design.sql 对齐，脚本内幂等自建）

约束（企业规范/实测经验）:
  - UNIQUE KEY 列必须为 schema 有序前缀
  - replication_num >= 2
  - 字段 COMMENT 不含引号
  - _s_d=每日快照(按dt留史,重跑同key覆盖)  _i_d=日增量(同日幂等不重复)
"""

DDL_SNAPSHOT = """
CREATE TABLE IF NOT EXISTS {db}.ods_datawind_dataset_s_d (
    dt                      DATE         NOT NULL COMMENT '数据日期(分区键)=抓取日',
    dataset_id              BIGINT       NOT NULL COMMENT '数据集ID(DataWind全局唯一)',
    etl_time                DATETIME     NOT NULL COMMENT '数据处理时间,本次ETL写入时刻(Asia/Shanghai)',
    name                    VARCHAR(512) COMMENT '数据集名称',
    owner_email             VARCHAR(128) COMMENT '负责人邮箱(ownerEmailPrefix, 如 llyunze@2105712913)',
    type_code               TINYINT      COMMENT '数据集类型编码: 22=prep数据集(加工型) 32=直连数据集(直接读外部表) 100=ClickHouse CDW数据集',
    type_name               VARCHAR(64)  COMMENT '数据集类型名: prep 数据集 / (直连,接口未回填) / ClickHouse CDW数据集',
    status                  TINYINT      COMMENT '数据集状态: 0=正常 1=彻底删除 2=回收站; 31=实测存在5个(疑为停用/异常,待确认)',
    sync_type               TINYINT      COMMENT '同步调度模式: 1=自动 2=手动; NULL=直连无调度',
    frequency               VARCHAR(24)  COMMENT '同步频率: daily=按天 hourly=按小时 every_ten_minutes=每10分钟; NULL=无调度',
    schedule_time           VARCHAR(16)  COMMENT '计划调度时刻 HH:MM, 如 09:30; hourly任务为0',
    schedule_day            VARCHAR(256) COMMENT '计划调度日: 0=每天; 小时级任务为0,1,2,...,23(逗号分隔全部小时); 周/月模式按平台定义',
    ttl                     INT          COMMENT '数据保留天数: 0=不清理, 1500约4年',
    connection_mode         TINYINT      COMMENT '连接模式: 0=prep加工型(有SQL加工) 1=直连型(直接读外部表)',
    sync_mode               TINYINT      COMMENT '同步模式(平台文档: 1=手动 2=自动; 实测接口均为0=默认未设置, 以sync_type为准)',
    partition_filter        VARCHAR(64)  COMMENT '分区默认过滤条件 partitionDefaultFilter, 如 day',
    confidentiality         VARCHAR(32)  COMMENT '数据密级(空=未设置)',
    last_sync_time          DATETIME     COMMENT '最近一次同步完成时间(数据层面)',
    last_success_task_time  DATETIME     COMMENT '最近一次成功任务时间(调度层面), 与上次比较判断是否有新数据产出',
    rows_cnt                BIGINT       COMMENT '最近一次成功任务产出行数(rowsForLastSuccessTaskTime), 与上次比较判断行数增减',
    elapsed_last_success    INT          COMMENT '最近一次成功任务耗时(秒, elapsedTimeForLastSuccessTaskTime)',
    upstream                VARCHAR(4000) COMMENT '上游来源表 db.table 逗号分隔(含Doris外部表 zamloan2_ct.xxx)',
    folder_id               BIGINT       COMMENT '所属文件夹ID(baseConf.parentId; 0/NULL=未归档根目录), 与dim_datawind_folder_s_d关联',
    folder_path             VARCHAR(512) COMMENT '文件夹路径(如 ZamLoan/zamloan2.0; 由folder_id解析文件夹树拼出)',
    is_hive_table           TINYINT      COMMENT '是否使用Hive表: 0=否 1=是',
    is_hive_partition       TINYINT      COMMENT '是否指定Hive分区: 0=否 1=是',
    hsql_used               TINYINT      COMMENT '是否使用HSQL加工: 0=否 1=是',
    url                     VARCHAR(1024) COMMENT '数据集控制台URL(可直接跳转详情页)',
    ctime                   DATETIME     COMMENT '数据集创建时间',
    mtime                   DATETIME     COMMENT '数据集元数据最后修改时间(变更检测重要信号)',
    sql_logic               TEXT         COMMENT '加工SQL逻辑(来自getDataModelInfo, 直连/上传类为空)',
    source_tables           TEXT         COMMENT '来源表列表(加工节点sourceTableList, 逗号分隔)',
    field_list              TEXT         COMMENT '字段清单JSON(name|dsColumnNames业务名|type)'
) ENGINE=OLAP
UNIQUE KEY(dt, dataset_id)
COMMENT 'DataWind数据集每日快照(s_d): 每日全量写入, dt分区+etl_time; 重跑先删当日分区再全量; 变更识别=今天vs昨天分区自比对'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(dataset_id) BUCKETS 16
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.replication_num" = "2",
    "replication_num" = "2"
)
"""

DDL_CHANGE = """
CREATE TABLE IF NOT EXISTS {db}.dwd_datawind_dataset_change_i_d (
    dt                  DATE         NOT NULL COMMENT '变更发生日(分区键)=快照日',
    change_id           BIGINT       NOT NULL COMMENT '变化序号(当日自增, 与dt构成唯一键, 重跑幂等)',
    etl_time            DATETIME     NOT NULL COMMENT '数据处理时间',
    dataset_id          BIGINT       NOT NULL COMMENT '数据集ID',
    dataset_name        VARCHAR(512) COMMENT '数据集名称(变更后)',
    change_type         VARCHAR(32)  COMMENT '变更类别编码: added=新增 deleted=删除 config_changed=配置修改 data_synced=数据同步更新 rows_changed=行数变化 upstream_changed=上游表变化 folder_changed=文件夹变更',
    change_type_name    VARCHAR(64)  COMMENT '变更类别中文名(与change_type一一对应)',
    changed_fields      VARCHAR(2000) COMMENT '发生变化的字段名列表(逗号分隔; 删除时=DELETED)',
    diff_json           TEXT         COMMENT '变更明细JSON(字段名->新旧值映射, 删除时=DELETED)',
    folder_path         VARCHAR(512) COMMENT '数据集所属文件夹路径(变更后)',
    remark              VARCHAR(512) COMMENT '备注(如: 连续N天未同步/行数异常波动等衍生告警)'
) ENGINE=OLAP
UNIQUE KEY(dt, change_id)
COMMENT 'DataWind数据集变更日志(i_d): 由快照自比对生成, 每类变化一条, 按日累积全量历史可追溯; 同日重跑幂等不重复'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(change_id) BUCKETS 4
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-90",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.replication_num" = "2",
    "replication_num" = "2"
)
"""

DDL_FOLDER = """
CREATE TABLE IF NOT EXISTS {db}.dim_datawind_folder_s_d (
    dt                      DATE         NOT NULL COMMENT '数据日期(分区键)=抓取日',
    folder_id               BIGINT       NOT NULL COMMENT '文件夹ID(DataWind全局唯一, public/private共用)',
    etl_time                DATETIME     NOT NULL COMMENT '数据处理时间',
    parent_id               BIGINT       COMMENT '父文件夹ID(NULL=顶层)',
    folder_name             VARCHAR(256) COMMENT '文件夹名称',
    folder_path             VARCHAR(1024) COMMENT '文件夹完整路径(如 ZamLoan/zamloan2.0; 由树逐级拼接)',
    namespace               VARCHAR(16)  COMMENT '命名空间: public=公共文件夹 private=私有文件夹'
) ENGINE=OLAP
UNIQUE KEY(dt, folder_id)
COMMENT 'DataWind文件夹树每日快照(s_d): 每日全量写入, dt分区+etl_time; 数据集folder_id关联此表解析folder_path'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(folder_id) BUCKETS 4
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.replication_num" = "2",
    "replication_num" = "2"
)
"""

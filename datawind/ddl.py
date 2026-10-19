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

DDL_TABLE_LINEAGE = """
CREATE TABLE IF NOT EXISTS {db}.dwd_datawind_table_lineage_t_d (
    dt                  DATE         NOT NULL COMMENT '血缘快照日(分区键)=抓取日',
    dataset_id          BIGINT       NOT NULL COMMENT '数据集ID(下游, 消费侧)',
    db_table            VARCHAR(256) NOT NULL COMMENT '上游来源表 db.table(如 mv.ods_zamloan_orderinfo_i_d; 无库名时=table)',
    lineage_type        VARCHAR(16)  COMMENT '血缘类型: direct=直接加工引用(加工SQL解析) api=接口上游表(sourceTableList)',
    depth               TINYINT      COMMENT '血缘层级(预留: 0=直接上游, N=多跳; 当前恒0, 闭包表负责多跳)',
    etl_time            DATETIME     NOT NULL COMMENT '数据处理时间'
) ENGINE=OLAP
UNIQUE KEY(dt, dataset_id, db_table)
COMMENT 'DataWind表级血缘(t_d): 数据集->上游表, SQLGlot解析+接口sourceTableList双源合并; 按dt累积可回溯历史血缘; 闭包表在此表基础上预计算多跳'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(dataset_id) BUCKETS 8
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-180",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.replication_num" = "2",
    "replication_num" = "2"
)
"""

DDL_COLUMN_LINEAGE = """
CREATE TABLE IF NOT EXISTS {db}.dwd_datawind_column_lineage_t_d (
    dt                  DATE         NOT NULL COMMENT '血缘快照日(分区键)=抓取日',
    dataset_id          BIGINT       NOT NULL COMMENT '数据集ID(下游)',
    target_field        VARCHAR(256) NOT NULL COMMENT '目标字段(数据集输出列, 加工SQL的SELECT别名)',
    src_table           VARCHAR(256) COMMENT '来源表 db.table(无表名=SQL内子查询/表达式, 留空)',
    src_field           VARCHAR(256) COMMENT '来源字段(无则=常量/聚合, 留空)',
    confidence          VARCHAR(8)   COMMENT '置信度: high=直接列引用(目标=源列一一对应) medium=表达式/函数/CTE推导 pending=无法提取(常量/复杂表达式), 前端分级展示',
    etl_time            DATETIME     NOT NULL COMMENT '数据处理时间'
) ENGINE=OLAP
UNIQUE KEY(dt, dataset_id, target_field, src_table, src_field)
COMMENT 'DataWind字段级血缘(t_d): 目标字段<-来源表.来源字段, SQLGlot解析加工SQL提取; 模型参考DataHub fine-grained lineage(边+置信度); pending边前端标灰供人工补'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(dataset_id) BUCKETS 8
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-180",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.replication_num" = "2",
    "replication_num" = "2"
)
"""

DDL_PARSE_STAT = """
CREATE TABLE IF NOT EXISTS {db}.dwd_datawind_parse_stat_s_d (
    dt                  DATE         NOT NULL COMMENT '统计日(分区键)=抓取日',
    etl_time            DATETIME     NOT NULL COMMENT '数据处理时间',
    total_cnt           INT          NOT NULL COMMENT '待解析数据集总数(有加工SQL)',
    parsed_cnt          INT          COMMENT '解析成功数(parse_status=ok或partial)',
    pending_cnt         INT          COMMENT '无加工SQL/直连数据集数(不参与解析)',
    fail_cnt            INT          COMMENT '解析失败数(所有方言均失败)',
    high_conf_cnt       INT          COMMENT '高置信字段血缘边数',
    medium_conf_cnt     INT          COMMENT '中置信字段血缘边数',
    pending_conf_cnt    INT          COMMENT 'pending字段血缘边数(需人工补)',
    table_edge_cnt      INT          COMMENT '表级血缘边总数(去重)',
    column_edge_cnt     INT          COMMENT '字段级血缘边总数(去重)'
) ENGINE=OLAP
UNIQUE KEY(dt)
COMMENT 'DataWind血缘解析质量统计(s_d): 每日一行, 观察解析率与置信度分布, 超阈值触发告警'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(dt) BUCKETS 2
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

DDL_CLOSURE = """
CREATE TABLE IF NOT EXISTS {db}.dwd_datawind_lineage_closure_t_d (
    dt                  DATE         NOT NULL COMMENT '闭包快照日(分区键)=抓取日',
    dataset_id          BIGINT       NOT NULL COMMENT '数据集ID(下游, 消费侧)',
    up_db               VARCHAR(128) COMMENT '上游表所属库名(db; 无库名时留空)',
    up_table            VARCHAR(256) NOT NULL COMMENT '上游表名(不含库, 与up_db合成db.table)',
    hop_depth           TINYINT      COMMENT '血缘跳数: 1=直接上游(加工SQL引用); >1=经中间数据集/表传递(预留, 当前源表依赖图未接入恒为1)',
    lineage_path        VARCHAR(2000) COMMENT '血缘路径(预留): 1跳为直接上游表; 多跳形如 表A->表B->数据集(源表间依赖图接入后填充)',
    etl_time            DATETIME     NOT NULL COMMENT '数据处理时间'
) ENGINE=OLAP
UNIQUE KEY(dt, dataset_id, up_db, up_table)
COMMENT 'DataWind血缘闭包(t_d): 数据集->上游表全路径预计算(当前1跳=table_lineage全量; 源表依赖图接入后可迭代多跳); 支撑上游全部来源查询与按up_table反查下游影响面, 避免运行时递归'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(dataset_id) BUCKETS 8
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-180",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.replication_num" = "2",
    "replication_num" = "2"
)
"""

DDL_DORIS_TABLE = """
CREATE TABLE IF NOT EXISTS {db}.ods_doris_table_s_d (
    dt                  DATE         NOT NULL COMMENT '资产快照日(分区键)=抓取日',
    db_name             VARCHAR(128) NOT NULL COMMENT '库名(Doris database; 如 mv/ods/dwd/dim/dws/ads/tmp/fin 等业务库)',
    table_name          VARCHAR(256) NOT NULL COMMENT '表名(不含库, 与db_name合成db.table唯一标识)',
    table_type          VARCHAR(16)  COMMENT '表类型枚举: BASE TABLE=物理表 VIEW=视图',
    engine              VARCHAR(32)  COMMENT '存储引擎(如 Doris)',
    rows_cnt            BIGINT       COMMENT '表行数估计值(information_schema.tables.TABLE_ROWS, 非精确)',
    data_size           BIGINT       COMMENT '表数据量字节(information_schema.tables.DATA_LENGTH)',
    avg_row_len         BIGINT       COMMENT '平均行字节数(估计值)',
    create_time         DATETIME     COMMENT '建表时间(元数据)',
    update_time         DATETIME     COMMENT '最近更新时间(元数据, 部分表可能为空/不准, 仅参考)',
    table_comment       VARCHAR(1024) COMMENT '表注释(建表COMMENT)',
    layer               VARCHAR(16)  COMMENT '数仓分层标签: mv/ods/dwd/dim/dws/ads/tmp/fin=按库名前缀识别; other=app/bak/etl/shield/stg/test等未归层库',
    etl_time            DATETIME     NOT NULL COMMENT '数据处理时间,本次ETL写入时刻(Asia/Shanghai)'
) ENGINE=OLAP
UNIQUE KEY(dt, db_name, table_name)
COMMENT 'Doris物理表资产每日快照(s_d): 每日从information_schema.tables采集全业务库, dt分区+etl_time; 用于资产目录展示Doris侧表资产, 与DataWind数据集快照互补; 支持按库/分层/类型筛选'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(db_name, table_name) BUCKETS 8
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

DDL_DORIS_COLUMN = """
CREATE TABLE IF NOT EXISTS {db}.ods_doris_column_s_d (
    dt                  DATE         NOT NULL COMMENT '资产快照日(分区键)=抓取日',
    db_name             VARCHAR(128) NOT NULL COMMENT '库名(与ods_doris_table_s_d.db_name一致)',
    table_name          VARCHAR(256) NOT NULL COMMENT '表名',
    column_name         VARCHAR(256) NOT NULL COMMENT '字段名',
    ordinal_position    INT          COMMENT '字段序号(表内自1递增, 建表顺序)',
    column_type         VARCHAR(128) COMMENT '字段类型(完整定义, 如 BIGINT/VARCHAR(256)/DECIMAL(20,2))',
    data_type           VARCHAR(64)  COMMENT '字段基础类型(不带长度精度, 如 BIGINT/VARCHAR/DECIMAL)',
    is_nullable         VARCHAR(8)   COMMENT '是否可空: YES=允许NULL NO=NOT NULL',
    column_default      VARCHAR(256) COMMENT '字段默认值(无默认值时可能为NULL)',
    column_comment      VARCHAR(1024) COMMENT '字段注释(建表COMMENT, 可为空)',
    etl_time            DATETIME     NOT NULL COMMENT '数据处理时间'
) ENGINE=OLAP
UNIQUE KEY(dt, db_name, table_name, column_name)
COMMENT 'Doris字段资产每日快照(s_d): 每日从information_schema.columns采集全业务库字段级元数据, dt分区+etl_time; 支撑字段搜索/详情/字段级资产盘点, 与血缘字段边关联'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(db_name, table_name) BUCKETS 8
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

DDL_DS_WORKFLOW = """
CREATE TABLE IF NOT EXISTS {db}.ods_ds_workflow_s_d (
    dt              DATE         NOT NULL COMMENT '数据日期(分区键)=抓取日',
    project_code    BIGINT       NOT NULL COMMENT 'DS项目编码(projects.code, 如18631453023104)',
    workflow_code   BIGINT       NOT NULL COMMENT '工作流定义编码(process-definition.code)',
    task_code       BIGINT       NOT NULL COMMENT '任务定义编码(taskDefinition.code)',
    project_name    VARCHAR(128) COMMENT 'DS项目名(如 ODS/DWD/DWS/ADS/STG/ETL, 按数仓分层组织)',
    workflow_name   VARCHAR(255) COMMENT '工作流名(业务线, 如 zamloan2/fastako/biumoney)',
    release_state   VARCHAR(16)  COMMENT '工作流发布状态: ONLINE=已上线 OFFLINE=已下线',
    task_name       VARCHAR(255) COMMENT '任务名=数仓产出表名(如 ods_zamloan2_member_i_d)',
    task_type       VARCHAR(32)  COMMENT '任务类型: SQL=SQL任务 SHELL=Shell任务(脚本/命令行)',
    task_flag       VARCHAR(16)  COMMENT '任务启用标志: YES=启用 NO=停用',
    sql_text        TEXT         COMMENT '任务SQL全文(建表+INSERT, SHELL任务为空)',
    etl_time        DATETIME     NOT NULL COMMENT '数据处理时间,本次ETL写入时刻(UTC)'
) ENGINE=OLAP
UNIQUE KEY(dt, project_code, workflow_code, task_code)
COMMENT 'DolphinScheduler调度任务资产每日快照(s_d): 任务=数仓产出表, 支撑调度资产盘点/任务清单/调度频率对齐与血缘SQL回查; dt分区+etl_time, 重跑先删当日分区再全量'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(workflow_code) BUCKETS 8
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

DDL_DS_LINEAGE = """
CREATE TABLE IF NOT EXISTS {db}.ods_ds_lineage_s_d (
    dt              DATE         NOT NULL COMMENT '数据日期(分区键)=抓取日',
    workflow_code   BIGINT       NOT NULL COMMENT '工作流定义编码',
    task_code       BIGINT       NOT NULL COMMENT '任务定义编码',
    dst_db          VARCHAR(64)  COMMENT '产出表库(INSERT目标, 如 ods)',
    dst_table       VARCHAR(256) COMMENT '产出表名(INSERT目标, 如 ods_zamloan2_member_i_d)',
    src_db          VARCHAR(64)  COMMENT '上游表库(SELECT来源, 可为空=use语句或未识别)',
    src_table       VARCHAR(256) COMMENT '上游表名(SELECT来源)',
    project_code    BIGINT       COMMENT 'DS项目编码',
    project_name    VARCHAR(128) COMMENT 'DS项目名',
    workflow_name   VARCHAR(255) COMMENT '工作流名',
    task_name       VARCHAR(255) COMMENT '任务名=产出表名',
    etl_time        DATETIME     NOT NULL COMMENT '数据处理时间,本次ETL写入时刻(UTC)'
) ENGINE=OLAP
UNIQUE KEY(dt, workflow_code, task_code, dst_db, dst_table, src_db, src_table)
COMMENT 'DolphinScheduler任务SQL表间血缘边每日快照(s_d): 解析任务SQL INSERT->SELECT 生成 Doris 表->表生产依赖, 补齐血缘图谱表的上游生产逻辑; dt分区+etl_time, 重跑先删当日分区再全量'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(workflow_code) BUCKETS 8
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

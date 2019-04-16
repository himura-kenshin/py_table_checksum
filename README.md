# py-table-checksum
多活数据中心 数据一致性校验模块

基于RDS-自建IDC 双活后数据双向同步

原理上参照了pt-table-checksum的实现，在一个数据中心源库上执行数据库快照，基于某一刻的数据库快照做checksum

目标库 同步了binlog后  目标端也完成checksum ,然后比对 源端目标端checksum的值是否一致

目前遇到的坑：

RDS  binlog只支持row格式 ，不支持statement格式 无法基于上述实现同一快照




package jp.co.apasys.table;

import jp.co.apasys.model.MergeTableInfo;

public class MergeSQL {

    private Integer maxBucket = 0;
    private Integer hashValue = 0;

    public MergeSQL(Integer maxBucket, Integer hashValue) {
	this.maxBucket = maxBucket - 1;
	this.hashValue = hashValue - 1;
    }

    public String getNewTableSql(MergeTableInfo mergeTableInfo) {
	StringBuilder sqlBuilder = new StringBuilder();
	sqlBuilder.append(" SELECT ");
	sqlBuilder.append(mergeTableInfo.getNewTableMergeColumn());
	sqlBuilder.append(" , ");
	sqlBuilder.append(" ROW_NUMBER() OVER (ORDER BY ");
	sqlBuilder.append(mergeTableInfo.getNewTableKeyColumn());
	sqlBuilder.append(" ) AS  rowNo ");
	sqlBuilder.append(" FROM ");
	sqlBuilder.append(mergeTableInfo.getNewTableNameString());
	sqlBuilder.append(" WHERE ");
	sqlBuilder.append(" AND ORA_HASH( ");
	sqlBuilder.append(mergeTableInfo.getNewTableKeyColumn().replaceAll(",",
		"||"));
	sqlBuilder.append(" , ");
	sqlBuilder.append(maxBucket);
	sqlBuilder.append(" ) = ");
	sqlBuilder.append(hashValue);
	sqlBuilder.append(mergeTableInfo.getNewTableMergeFilter());
	sqlBuilder.append(" ORDER BY ");
	sqlBuilder.append(mergeTableInfo.getNewTableKeyColumn());
	
	return new String(sqlBuilder);
    }

    public String getOldTableSql(MergeTableInfo mergeTableInfo) {
	StringBuilder sqlBuilder = new StringBuilder();
	sqlBuilder.append(" SELECT ");
	sqlBuilder.append(mergeTableInfo.getOldTableMergeColumn());
	sqlBuilder.append(" , ");
	sqlBuilder.append(" ROW_NUMBER() OVER (ORDER BY ");
	sqlBuilder.append(mergeTableInfo.getOldTableKeyColumn());
	sqlBuilder.append(" ) AS  rowNo ");
	sqlBuilder.append(" FROM ");
	sqlBuilder.append(mergeTableInfo.getOldTableNameString());
	sqlBuilder.append(" WHERE ");
	sqlBuilder.append(" AND ORA_HASH( ");
	sqlBuilder.append(mergeTableInfo.getOldTableKeyColumn().replaceAll(",",
		"||"));
	sqlBuilder.append(" , ");
	sqlBuilder.append(maxBucket);
	sqlBuilder.append(" ) = ");
	sqlBuilder.append(hashValue);
	sqlBuilder.append(mergeTableInfo.getOldTableMergeFilter());
	sqlBuilder.append(" ORDER BY ");
	sqlBuilder.append(mergeTableInfo.getOldTableKeyColumn());
	
	return new String(sqlBuilder);
    }

}

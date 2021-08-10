package com.opendigitaleducation.events.postgresql;

import java.time.LocalDateTime;

class PartitionTable {

    private final String parentTableName;
    private final String tableName;
    private final LocalDateTime dbEndRange;
    private final LocalDateTime startRange;
    private final LocalDateTime endRange;

    public PartitionTable(String parentTableName, String tableName, LocalDateTime startRange,
            LocalDateTime endRange, LocalDateTime dbEndRange) {
        this.parentTableName = parentTableName;
        this.tableName = tableName;
        this.startRange = startRange;
        this.endRange = endRange;
        this.dbEndRange = dbEndRange;
    }

    public LocalDateTime getDbEndRange() {
        return dbEndRange;
    }

    public LocalDateTime getEndRange() {
        return endRange;
    }

    public LocalDateTime getStartRange() {
        return startRange;
    }

    public String getTableName() {
        return tableName;
    }

    public String getParentTableName() {
        return parentTableName;
    }

}

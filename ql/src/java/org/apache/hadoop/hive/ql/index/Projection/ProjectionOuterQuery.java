package org.apache.hadoop.hive.ql.index.Projection;

import org.apache.hadoop.hive.ql.index.bitmap.BitmapQuery;

/**
 * Created by samadhik on 1/13/17.
 */
public class ProjectionOuterQuery implements  BitmapQuery {
    private String alias;
    private BitmapQuery lhs;
    private BitmapQuery rhs;
    private String queryStr;

    public ProjectionOuterQuery(String alias, BitmapQuery lhs, BitmapQuery rhs) {

        this.alias = alias;
        this.lhs = lhs;
        this.rhs = rhs;
        constructQueryStr();
    }

    public String getAlias() {
        return alias;
    }

    /**
     * Return a string representation of the query for compilation
     */
    public String toString() {
        return queryStr;
    }

    /**
     * Construct a string representation of the query to be compiled
     */
    private void constructQueryStr() {
        StringBuilder sb = new StringBuilder();
        sb.append("(SELECT ");
        sb.append(lhs.getAlias());
        sb.append(".`_bucketname` AS `_bucketname`, ");
        //sb.append(rhs.getAlias());
        //sb.append(".`_offset`, ");
        sb.append("EWAH_BITMAP_AND(");
        sb.append(lhs.getAlias());
        sb.append(".`_bitmaps`, ");
        sb.append(rhs.getAlias());
        sb.append(".`_bitmaps`) AS `_bitmaps` FROM ");
        sb.append(lhs.toString());
        sb.append(" FULL OUTER JOIN ");//WASam edit change JOIN to full outer join
        sb.append(rhs.toString());
        //sb.append(" ON ");
        //sb.append(lhs.getAlias());
        //sb.append(".`_bucketname` = ");
        //sb.append(rhs.getAlias());
        //sb.append(".`_bucketname` AND ");
        //sb.append(lhs.getAlias());
        //sb.append(".`_offset` = ");
        //sb.append(rhs.getAlias());
        //sb.append(".`_offset`) ");
        //sb.append(this.alias);
        queryStr = sb.toString();
    }

}

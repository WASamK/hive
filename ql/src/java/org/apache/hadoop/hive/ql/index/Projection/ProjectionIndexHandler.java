package org.apache.hadoop.hive.ql.index.Projection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.index.*;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapIndexHandler;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapInnerQuery;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapOuterQuery;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapQuery;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.IndexUtils;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCollectSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by samadhik on 1/2/17.
 */
public class ProjectionIndexHandler extends TableBasedIndexHandler {

    private Configuration configuration;
    private static final Logger LOG = LoggerFactory.getLogger(BitmapIndexHandler.class.getName());

    @Override
    public void generateIndexQuery(List<Index> indexes, ExprNodeDesc predicate,
                                   ParseContext pctx, HiveIndexQueryContext queryContext) {
        Map<Index, ExprNodeDesc> indexPredicates  = decomposePredicate(
                predicate,
                indexes,
                queryContext);

        if (indexPredicates == null) {
            LOG.info("No decomposed predicate found");
            queryContext.setQueryTasks(null);
            return; // abort if we couldn't pull out anything from the predicate
        }

        List<BitmapInnerQuery> iqs = new ArrayList<BitmapInnerQuery>(indexes.size());
        int i = 0;
        for (Index index : indexes) {
            ExprNodeDesc indexPredicate = indexPredicates.get(index);
            if (indexPredicate != null) {
                iqs.add(new BitmapInnerQuery(
                        index.getIndexTableName(),
                        indexPredicate,
                        "ind" + i++));
                LOG.info("SAM inner query string "+iqs.toString()); //WASam edit
            }
        }
        // setup TableScanOperator to change input format for original query
        queryContext.setIndexInputFormat(HiveIndexedInputFormat.class.getName());

        // Build reentrant QL for index query
        StringBuilder qlCommand = new StringBuilder("INSERT OVERWRITE DIRECTORY "); //WASam edit was to comment this

        String tmpFile = pctx.getContext().getMRTmpPath().toUri().toString();
        qlCommand.append( "\"" + tmpFile + "\" ");            // QL includes " around file name //WASam edit was to comment this
        qlCommand.append("SELECT tmp_index.bucketname AS `_bucketname` , EWAH_BITMAP_POSITIONS(tmp_index.bitmaps) AS `_offsets` FROM ");
        qlCommand.append("(SELECT a.`_bucketname` AS bucketname ,");
        qlCommand.append(" a.`_bitmaps` AS bitmaps FROM ");


        BitmapQuery head = iqs.get(0);
        LOG.info("SAM head : "+head.toString());//WASam edit
        for ( i = 1; i < iqs.size(); i++) {
            head = new ProjectionOuterQuery("oind"+i, head, iqs.get(i));
            LOG.info("SAM iqs.get(i) : "+iqs.get(i).toString());//WASam edit
        }
        qlCommand.append(head.toString());
        //qlCommand.append(" ) a");
        qlCommand.append(" )a ) tmp_index GROUP BY bucketname,bitmaps");//WHERE NOT EWAH_BITMAP_EMPTY(" + head.getAlias() + ".`_bitmaps`)

        String lines = qlCommand.toString();//WASam edit
        LOG.info("SAM qlCommand: "+lines);//WASam edit

        // generate tasks from index query string
        LOG.info("Generating tasks for re-entrant QL query: " + qlCommand.toString());
        HiveConf queryConf = new HiveConf(pctx.getConf(), BitmapIndexHandler.class);
        HiveConf.setBoolVar(queryConf, HiveConf.ConfVars.COMPRESSRESULT, false);
        Driver driver = new Driver(queryConf);
        driver.compile(qlCommand.toString(),false);  //WASam edit was to comment this

        queryContext.setIndexIntermediateFile(tmpFile); //WASam edit was to comment this
        queryContext.addAdditionalSemanticInputs(driver.getPlan().getInputs()); //WASam edit was to comment this
        queryContext.setQueryTasks(driver.getPlan().getRootTasks()); //WASam edit was to comment this


    /*WASam edit
    try {
      driver.run(qlCommand.toString());
    } catch (CommandNeedRetryException e) {
      e.printStackTrace();
    }
    ArrayList<String> res = new ArrayList<String>();

    try {
      while (driver.getResults(res)) {
        for (String r : res) {
          LOG.info("Poor : "+r);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (CommandNeedRetryException e) {
      e.printStackTrace();
    }


    /*WASam edit*/

    }

    /**
     * Split the predicate into the piece we can deal with (pushed), and the one we can't (residual)
     * @param predicate
     * @param index
     * @return
     */
    private Map<Index, ExprNodeDesc> decomposePredicate(ExprNodeDesc predicate, List<Index> indexes,
                                                        HiveIndexQueryContext queryContext) {

        Map<Index, ExprNodeDesc> indexPredicates = new HashMap<Index, ExprNodeDesc>();
        // compute overall residual
        IndexPredicateAnalyzer analyzer = getIndexPredicateAnalyzer(indexes, queryContext.getQueryPartitions());
        List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
        ExprNodeDesc residualPredicate = analyzer.analyzePredicate(predicate, searchConditions);
        // pass residual predicate back out for further processing
        queryContext.setResidualPredicate(residualPredicate);

        if (searchConditions.size() == 0) {
            return null;
        }

        for (Index index : indexes) {
            ArrayList<Index> in = new ArrayList<Index>(1);
            in.add(index);
            analyzer = getIndexPredicateAnalyzer(in, queryContext.getQueryPartitions());
            searchConditions = new ArrayList<IndexSearchCondition>();
            // split predicate into pushed (what we can handle), and residual (what we can't handle)
            // pushed predicate from translateSearchConditions is stored for the current index
            // This ensures that we apply all possible predicates to each index
            analyzer.analyzePredicate(predicate, searchConditions);
            if (searchConditions.size() == 0) {
                indexPredicates.put(index, null);
            } else {
                indexPredicates.put(index, analyzer.translateSearchConditions(searchConditions));
            }
        }

        return indexPredicates;
    }

    /**
     * Instantiate a new predicate analyzer suitable for determining
     * whether we can use an index, based on rules for indexes in
     * WHERE clauses that we support
     *
     * @return preconfigured predicate analyzer for WHERE queries
     */
    private IndexPredicateAnalyzer getIndexPredicateAnalyzer(List<Index> indexes, Set<Partition> queryPartitions)  {
        IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();

        analyzer.addComparisonOp(GenericUDFOPEqual.class.getName());
        analyzer.addComparisonOp(GenericUDFOPLessThan.class.getName());
        analyzer.addComparisonOp(GenericUDFOPEqualOrLessThan.class.getName());
        analyzer.addComparisonOp(GenericUDFOPGreaterThan.class.getName());
        analyzer.addComparisonOp(GenericUDFOPEqualOrGreaterThan.class.getName());

        // only return results for columns in the list of indexes
        for (Index index : indexes) {
            List<FieldSchema> columnSchemas = index.getSd().getCols();
            for (FieldSchema column : columnSchemas) {
                analyzer.allowColumnName(column.getName());
            }
        }

        // partitioned columns are treated as if they have indexes so that the partitions
        // are used during the index query generation
        for (Partition part : queryPartitions) {
            if (part.getSpec().isEmpty()) {
                continue; // empty partitions are from whole tables, so we don't want to add them in
            }
            for (String column : part.getSpec().keySet()) {
                analyzer.allowColumnName(column);
            }
        }

        return analyzer;
    }

    @Override
    public void analyzeIndexDefinition(Table baseTable, Index index,
                                       Table indexTable) throws HiveException {
        StorageDescriptor storageDesc = index.getSd();
        if (this.usesIndexTable() && indexTable != null) {
            StorageDescriptor indexTableSd = storageDesc.deepCopy();
            List<FieldSchema> indexTblCols = indexTableSd.getCols();
            FieldSchema bucketFileName = new FieldSchema("_bucketname", "string", "");
            indexTblCols.add(bucketFileName);
            FieldSchema bitmaps = new FieldSchema("_bitmaps", "array<bigint>", "");
            indexTblCols.add(bitmaps);
            indexTable.setSd(indexTableSd);
        }
    }

    @Override
    protected Task<?> getIndexBuilderMapRedTask(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
                                                List<FieldSchema> indexField, boolean partitioned,
                                                PartitionDesc indexTblPartDesc, String indexTableName,
                                                PartitionDesc baseTablePartDesc, String baseTableName, String dbName) throws HiveException {



        HiveConf builderConf = new HiveConf(getConf(), BitmapIndexHandler.class);
        HiveConf.setBoolVar(builderConf, HiveConf.ConfVars.HIVEROWOFFSET, true);

        String indexCols = HiveUtils.getUnparsedColumnNamesFromFieldSchema(indexField);

        //form a new insert overwrite query.
        StringBuilder command= new StringBuilder();
        LinkedHashMap<String, String> partSpec = indexTblPartDesc.getPartSpec();

        command.append("INSERT OVERWRITE TABLE " +
                HiveUtils.unparseIdentifier(dbName) + "." + HiveUtils.unparseIdentifier(indexTableName ));
        if (partitioned && indexTblPartDesc != null) {
            command.append(" PARTITION ( ");
            List<String> ret = getPartKVPairStringArray(partSpec);
            for (int i = 0; i < ret.size(); i++) {
                String partKV = ret.get(i);
                command.append(partKV);
                if (i < ret.size() - 1) {
                    command.append(",");
                }
            }
            command.append(" ) ");
        }

        command.append(" SELECT ");
        command.append(indexCols);
        command.append(",");
        command.append(VirtualColumn.FILENAME.getName());
        command.append(",");
        //command.append(VirtualColumn.BLOCKOFFSET.getName());
        //command.append(",");
        command.append("EWAH_BITMAP(");
        //command.append(VirtualColumn.ROWOFFSET.getName());
        command.append(VirtualColumn.BLOCKOFFSET.getName());
        command.append(")");
        command.append(" FROM " +
                HiveUtils.unparseIdentifier(dbName) + "." + HiveUtils.unparseIdentifier(baseTableName));
        LinkedHashMap<String, String> basePartSpec = baseTablePartDesc.getPartSpec();
        if(basePartSpec != null) {
            command.append(" WHERE ");
            List<String> pkv = getPartKVPairStringArray(basePartSpec);
            for (int i = 0; i < pkv.size(); i++) {
                String partKV = pkv.get(i);
                command.append(partKV);
                if (i < pkv.size() - 1) {
                    command.append(" AND ");
                }
            }
        }
        command.append(" GROUP BY ");
        command.append(indexCols + ", " + VirtualColumn.FILENAME.getName());
        for (FieldSchema fieldSchema : indexField) {
            command.append(",");
            command.append(HiveUtils.unparseIdentifier(fieldSchema.getName()));
        }

        // Require clusterby ROWOFFSET if map-size aggregation is off.
        // TODO: Make this work without map side aggregation
        if (!builderConf.get("hive.map.aggr", null).equals("true")) {
            throw new HiveException("Cannot construct index without map-side aggregation");
        }

        String str=""; //WASam edit
        String[] lines = command.toString().split("\\n");
        for(String s: lines){
            str+=s;
        }
        LOG.info("SAM : getIndexBuilderMapRedTask string : "+str);

        Task<?> rootTask = IndexUtils.createRootTask(builderConf, inputs, outputs,
                command, partSpec, indexTableName, dbName);
        return rootTask;
    }

    @Override
    /**
     * No lower bound on bitmap index query size, so this will always return true
     */
    public boolean checkQuerySize(long querySize, HiveConf hiveConf) {
        LOG.info("SAM : checkQuerySize ");
        return true;
    }

    @Override
    public boolean usesIndexTable() {
        LOG.info("SAM : usesIndexTable() : "); //WASam edit
        return true;
    }

}

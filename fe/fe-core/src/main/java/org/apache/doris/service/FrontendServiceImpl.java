// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.service;

import org.apache.doris.analysis.AbstractBackupTableRefClause;
import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.PartitionExprUtil;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TableSample;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.backup.Snapshot;
import org.apache.doris.binlog.BinlogLagInfo;
import org.apache.doris.catalog.AutoIncrementGenerator;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.View;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.proto.Cloud.CommitTxnResponse;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.GZIPUtils;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.Status;
import org.apache.doris.common.ThriftServerContext;
import org.apache.doris.common.ThriftServerEventProcessor;
import org.apache.doris.common.UserException;
import org.apache.doris.common.Version;
import org.apache.doris.common.annotation.LogException;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.cooldown.CooldownDelete;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.SplitSource;
import org.apache.doris.insertoverwrite.InsertOverwriteManager;
import org.apache.doris.insertoverwrite.InsertOverwriteUtil;
import org.apache.doris.load.StreamLoadHandler;
import org.apache.doris.load.routineload.ErrorReason;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.master.MasterImpl;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanNodeAndHash;
import org.apache.doris.nereids.trees.plans.commands.RestoreCommand;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableRefInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.plsql.metastore.PlsqlPackage;
import org.apache.doris.plsql.metastore.PlsqlProcedureKey;
import org.apache.doris.plsql.metastore.PlsqlStoredProcedure;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.HttpStreamParams;
import org.apache.doris.qe.MasterCatalogExecutor;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.qe.MysqlConnectProcessor;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.service.arrowflight.FlightSqlConnectProcessor;
import org.apache.doris.statistics.AnalysisManager;
import org.apache.doris.statistics.ColStatsData;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.InvalidateStatsTarget;
import org.apache.doris.statistics.StatisticsCacheKey;
import org.apache.doris.statistics.TableStatsMeta;
import org.apache.doris.statistics.UpdatePartitionStatsTarget;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatistics;
import org.apache.doris.statistics.query.QueryStats;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.tablefunction.MetadataGenerator;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.FrontendServiceVersion;
import org.apache.doris.thrift.TAddPlsqlPackageRequest;
import org.apache.doris.thrift.TAddPlsqlStoredProcedureRequest;
import org.apache.doris.thrift.TAutoIncrementRangeRequest;
import org.apache.doris.thrift.TAutoIncrementRangeResult;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TBeginTxnRequest;
import org.apache.doris.thrift.TBeginTxnResult;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TCheckAuthRequest;
import org.apache.doris.thrift.TCheckAuthResult;
import org.apache.doris.thrift.TColumnDef;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TColumnInfo;
import org.apache.doris.thrift.TCommitTxnRequest;
import org.apache.doris.thrift.TCommitTxnResult;
import org.apache.doris.thrift.TConfirmUnusedRemoteFilesRequest;
import org.apache.doris.thrift.TConfirmUnusedRemoteFilesResult;
import org.apache.doris.thrift.TCreatePartitionRequest;
import org.apache.doris.thrift.TCreatePartitionResult;
import org.apache.doris.thrift.TDescribeTablesParams;
import org.apache.doris.thrift.TDescribeTablesResult;
import org.apache.doris.thrift.TDropPlsqlPackageRequest;
import org.apache.doris.thrift.TDropPlsqlStoredProcedureRequest;
import org.apache.doris.thrift.TFeResult;
import org.apache.doris.thrift.TFetchResourceResult;
import org.apache.doris.thrift.TFetchRoutineLoadJobRequest;
import org.apache.doris.thrift.TFetchRoutineLoadJobResult;
import org.apache.doris.thrift.TFetchRunningQueriesRequest;
import org.apache.doris.thrift.TFetchRunningQueriesResult;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TFetchSplitBatchRequest;
import org.apache.doris.thrift.TFetchSplitBatchResult;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TFrontendPingFrontendRequest;
import org.apache.doris.thrift.TFrontendPingFrontendResult;
import org.apache.doris.thrift.TFrontendPingFrontendStatusCode;
import org.apache.doris.thrift.TFrontendReportAliveSessionRequest;
import org.apache.doris.thrift.TFrontendReportAliveSessionResult;
import org.apache.doris.thrift.TGetBackendMetaRequest;
import org.apache.doris.thrift.TGetBackendMetaResult;
import org.apache.doris.thrift.TGetBinlogLagResult;
import org.apache.doris.thrift.TGetBinlogRequest;
import org.apache.doris.thrift.TGetBinlogResult;
import org.apache.doris.thrift.TGetColumnInfoRequest;
import org.apache.doris.thrift.TGetColumnInfoResult;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetDbsResult;
import org.apache.doris.thrift.TGetMasterTokenRequest;
import org.apache.doris.thrift.TGetMasterTokenResult;
import org.apache.doris.thrift.TGetMetaDB;
import org.apache.doris.thrift.TGetMetaRequest;
import org.apache.doris.thrift.TGetMetaResult;
import org.apache.doris.thrift.TGetMetaTable;
import org.apache.doris.thrift.TGetQueryStatsRequest;
import org.apache.doris.thrift.TGetSnapshotRequest;
import org.apache.doris.thrift.TGetSnapshotResult;
import org.apache.doris.thrift.TGetTablesParams;
import org.apache.doris.thrift.TGetTablesResult;
import org.apache.doris.thrift.TGetTabletReplicaInfosRequest;
import org.apache.doris.thrift.TGetTabletReplicaInfosResult;
import org.apache.doris.thrift.TGroupCommitInfo;
import org.apache.doris.thrift.TInitExternalCtlMetaRequest;
import org.apache.doris.thrift.TInitExternalCtlMetaResult;
import org.apache.doris.thrift.TInvalidateFollowerStatsCacheRequest;
import org.apache.doris.thrift.TListPrivilegesResult;
import org.apache.doris.thrift.TListTableMetadataNameIdsResult;
import org.apache.doris.thrift.TListTableStatusResult;
import org.apache.doris.thrift.TLoadTxn2PCRequest;
import org.apache.doris.thrift.TLoadTxn2PCResult;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TLoadTxnCommitRequest;
import org.apache.doris.thrift.TLoadTxnCommitResult;
import org.apache.doris.thrift.TLoadTxnRollbackRequest;
import org.apache.doris.thrift.TLoadTxnRollbackResult;
import org.apache.doris.thrift.TLockBinlogRequest;
import org.apache.doris.thrift.TLockBinlogResult;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TMasterResult;
import org.apache.doris.thrift.TMySqlLoadAcquireTokenResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TNodeInfo;
import org.apache.doris.thrift.TNullableStringLiteral;
import org.apache.doris.thrift.TOlapTableIndexTablets;
import org.apache.doris.thrift.TOlapTablePartition;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TPlsqlPackageResult;
import org.apache.doris.thrift.TPlsqlStoredProcedureResult;
import org.apache.doris.thrift.TPrivilegeCtrl;
import org.apache.doris.thrift.TPrivilegeHier;
import org.apache.doris.thrift.TPrivilegeStatus;
import org.apache.doris.thrift.TPrivilegeType;
import org.apache.doris.thrift.TQueryStatsResult;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TReplacePartitionRequest;
import org.apache.doris.thrift.TReplacePartitionResult;
import org.apache.doris.thrift.TReplicaInfo;
import org.apache.doris.thrift.TReportCommitTxnResultRequest;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TReportExecStatusResult;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TRestoreSnapshotRequest;
import org.apache.doris.thrift.TRestoreSnapshotResult;
import org.apache.doris.thrift.TRollbackTxnRequest;
import org.apache.doris.thrift.TRollbackTxnResult;
import org.apache.doris.thrift.TRoutineLoadJob;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TSchemaTableName;
import org.apache.doris.thrift.TShowProcessListRequest;
import org.apache.doris.thrift.TShowProcessListResult;
import org.apache.doris.thrift.TShowUserRequest;
import org.apache.doris.thrift.TShowUserResult;
import org.apache.doris.thrift.TShowVariableRequest;
import org.apache.doris.thrift.TShowVariableResult;
import org.apache.doris.thrift.TSnapshotLoaderReportRequest;
import org.apache.doris.thrift.TSnapshotType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadMultiTablePutResult;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;
import org.apache.doris.thrift.TSubTxnInfo;
import org.apache.doris.thrift.TSyncQueryColumns;
import org.apache.doris.thrift.TTableIndexQueryStats;
import org.apache.doris.thrift.TTableMetadataNameIds;
import org.apache.doris.thrift.TTableQueryStats;
import org.apache.doris.thrift.TTableRef;
import org.apache.doris.thrift.TTableStatus;
import org.apache.doris.thrift.TTabletLocation;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUpdateExportTaskStatusRequest;
import org.apache.doris.thrift.TUpdateFollowerPartitionStatsCacheRequest;
import org.apache.doris.thrift.TUpdateFollowerStatsCacheRequest;
import org.apache.doris.thrift.TUpdatePlanStatsCacheRequest;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.SubTransactionState;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.transaction.TxnCommitAttachment;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

// Frontend service used to serve all request for this frontend through
// thrift protocol
public class FrontendServiceImpl implements FrontendService.Iface {
    private static final Logger LOG = LogManager.getLogger(FrontendServiceImpl.class);

    private static final String NOT_MASTER_ERR_MSG = "FE is not master";

    private MasterImpl masterImpl;
    private ExecuteEnv exeEnv;
    // key is txn id,value is index of plan fragment instance, it's used by multi
    // table request plan
    private ConcurrentHashMap<Long, AtomicInteger> multiTableFragmentInstanceIdIndexMap =
            new ConcurrentHashMap<>(64);

    private final Map<TUniqueId, ConnectContext> proxyQueryIdToConnCtx =
            new ConcurrentHashMap<>(64);

    private static TNetworkAddress getMasterAddress() {
        Env env = Env.getCurrentEnv();
        String masterHost = env.getMasterHost();
        int masterRpcPort = env.getMasterRpcPort();
        return new TNetworkAddress(masterHost, masterRpcPort);
    }

    public FrontendServiceImpl(ExecuteEnv exeEnv) {
        masterImpl = new MasterImpl();
        this.exeEnv = exeEnv;
    }

    @Override
    public TConfirmUnusedRemoteFilesResult confirmUnusedRemoteFiles(TConfirmUnusedRemoteFilesRequest request)
            throws TException {
        if (!Env.getCurrentEnv().isMaster()) {
            throw new TException("FE is not master");
        }
        TConfirmUnusedRemoteFilesResult res = new TConfirmUnusedRemoteFilesResult();
        if (!request.isSetConfirmList()) {
            throw new TException("confirm_list in null");
        }
        request.getConfirmList().forEach(info -> {
            if (!info.isSetCooldownMetaId()) {
                LOG.warn("cooldown_meta_id is null");
                return;
            }
            TabletMeta tabletMeta = Env.getCurrentEnv().getTabletInvertedIndex().getTabletMeta(info.tablet_id);
            if (tabletMeta == null) {
                LOG.warn("tablet {} not found", info.tablet_id);
                return;
            }
            Tablet tablet;
            int replicaNum;
            try {
                OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbNullable(tabletMeta.getDbId())
                        .getTable(tabletMeta.getTableId())
                        .get();
                table.readLock();
                replicaNum = table.getPartitionInfo().getReplicaAllocation(tabletMeta.getPartitionId())
                        .getTotalReplicaNum();
                try {
                    tablet = table.getPartition(tabletMeta.getPartitionId()).getIndex(tabletMeta.getIndexId())
                            .getTablet(info.tablet_id);
                } finally {
                    table.readUnlock();
                }
            } catch (RuntimeException e) {
                LOG.warn("tablet {} not found", info.tablet_id);
                return;
            }
            // check cooldownReplicaId
            Pair<Long, Long> cooldownConf = tablet.getCooldownConf();
            if (cooldownConf.first != info.cooldown_replica_id) {
                LOG.info("cooldown replica id not match({} vs {}), tablet={}", cooldownConf.first,
                        info.cooldown_replica_id, info.tablet_id);
                return;
            }
            // check cooldownMetaId of all replicas are the same
            List<Replica> replicas = Env.getCurrentEnv().getTabletInvertedIndex().getReplicas(info.tablet_id);
            // FIXME(plat1ko): We only delete remote files when tablet is under a stable
            // state: enough replicas and
            // all replicas are alive. Are these conditions really sufficient or necessary?
            if (replicas.size() < replicaNum) {
                LOG.info("num replicas are not enough, tablet={}", info.tablet_id);
                return;
            }
            for (Replica replica : replicas) {
                if (!replica.isAlive()) {
                    LOG.info("replica is not alive, tablet={}, replica={}", info.tablet_id, replica.getId());
                    return;
                }
                if (replica.getCooldownTerm() != cooldownConf.second) {
                    LOG.info("replica's cooldown term not match({} vs {}), tablet={}", cooldownConf.second,
                            replica.getCooldownTerm(), info.tablet_id);
                    return;
                }
                if (!info.cooldown_meta_id.equals(replica.getCooldownMetaId())) {
                    LOG.info("cooldown meta id are not same, tablet={}", info.tablet_id);
                    return;
                }
            }
            res.addToConfirmedTablets(info.tablet_id);
        });

        if (res.isSetConfirmedTablets() && !res.getConfirmedTablets().isEmpty()) {
            if (Env.getCurrentEnv().isMaster()) {
                // ensure FE is real master
                Env.getCurrentEnv().getEditLog().logCooldownDelete(new CooldownDelete());
            } else {
                throw new TException("FE is not master");
            }
        }

        return res;
    }

    @Override
    public TGetDbsResult getDbNames(TGetDbsParams params) throws TException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("get db request: {}", params);
        }
        TGetDbsResult result = new TGetDbsResult();

        List<String> dbNames = Lists.newArrayList();
        List<String> catalogNames = Lists.newArrayList();
        List<Long> dbIds = Lists.newArrayList();
        List<Long> catalogIds = Lists.newArrayList();

        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                        CaseSensibility.DATABASE.getCaseSensibility());
            } catch (PatternMatcherException e) {
                throw new TException("Pattern is in bad format: " + params.getPattern());
            }
        }

        Env env = Env.getCurrentEnv();
        List<CatalogIf> catalogIfs = Lists.newArrayList();
        // list all catalogs or the specified catalog.
        if (Strings.isNullOrEmpty(params.catalog)) {
            catalogIfs = env.getCatalogMgr().listCatalogs();
        } else {
            catalogIfs.add(env.getCatalogMgr()
                    .getCatalogOrException(params.catalog,
                            catalog -> new TException("Unknown catalog " + catalog)));
        }

        for (CatalogIf catalog : catalogIfs) {
            Collection<DatabaseIf> dbs = new HashSet<DatabaseIf>();
            try {
                dbs = catalog.getAllDbs();
            } catch (Exception e) {
                LOG.warn("failed to get database names for catalog {}", catalog.getName(), e);
                // Some external catalog may fail to get databases due to wrong connection info.
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("get db size: {}, in catalog: {}", dbs.size(), catalog.getName());
            }
            if (dbs.isEmpty() && params.isSetGetNullCatalog() && params.get_null_catalog) {
                catalogNames.add(catalog.getName());
                dbNames.add("NULL");
                catalogIds.add(catalog.getId());
                dbIds.add(-1L);
                continue;
            }
            if (dbs.isEmpty()) {
                continue;
            }
            UserIdentity currentUser = null;
            if (params.isSetCurrentUserIdent()) {
                currentUser = UserIdentity.fromThrift(params.current_user_ident);
            } else {
                currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
            }
            for (DatabaseIf db : dbs) {
                String dbName = db.getFullName();
                if (!env.getAccessManager()
                        .checkDbPriv(currentUser, catalog.getName(), dbName, PrivPredicate.SHOW)) {
                    continue;
                }

                if (matcher != null && !matcher.match(getMysqlTableSchema(catalog.getName(), dbName))) {
                    continue;
                }

                catalogNames.add(catalog.getName());
                dbNames.add(getMysqlTableSchema(catalog.getName(), dbName));
                catalogIds.add(catalog.getId());
                dbIds.add(db.getId());
            }
        }

        result.setDbs(dbNames);
        result.setCatalogs(catalogNames);
        result.setCatalogIds(catalogIds);
        result.setDbIds(dbIds);
        return result;
    }

    private String getMysqlTableSchema(String ctl, String db) {
        if (!GlobalVariable.showFullDbNameInInfoSchemaDb) {
            return db;
        }
        if (ctl.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            return db;
        }
        return ctl + "." + db;
    }

    private String getDbNameFromMysqlTableSchema(String ctl, String db) {
        if (ctl.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            return db;
        }
        String[] parts = db.split("\\.");
        if (parts.length == 2) {
            return parts[1];
        }
        return db;
    }

    @LogException
    @Override
    public TGetTablesResult getTableNames(TGetTablesParams params) throws TException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("get table name request: {}", params);
        }
        TGetTablesResult result = new TGetTablesResult();
        List<String> tablesResult = Lists.newArrayList();
        result.setTables(tablesResult);
        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            } catch (PatternMatcherException e) {
                throw new TException("Pattern is in bad format: " + params.getPattern());
            }
        }

        // database privs should be checked in analysis phrase
        UserIdentity currentUser;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        String catalogName = Strings.isNullOrEmpty(params.catalog) ? InternalCatalog.INTERNAL_CATALOG_NAME
                : params.catalog;
        String dbName = getDbNameFromMysqlTableSchema(catalogName, params.db);
        DatabaseIf<TableIf> db = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrException(catalogName, catalog -> new TException("Unknown catalog: " + catalog))
                .getDbNullable(dbName);

        if (db != null) {
            Set<String> tableNames;
            try {
                tableNames = db.getTableNamesOrEmptyWithLock();
                for (String tableName : tableNames) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("get table: {}, wait to check", tableName);
                    }
                    if (!Env.getCurrentEnv().getAccessManager()
                            .checkTblPriv(currentUser, catalogName, dbName, tableName,
                                    PrivPredicate.SHOW)) {
                        continue;
                    }
                    if (matcher != null && !matcher.match(tableName)) {
                        continue;
                    }
                    tablesResult.add(tableName);
                }
            } catch (Exception e) {
                LOG.warn("failed to get table names for db {} in catalog {}", params.db, catalogName, e);
            }
        }
        return result;
    }

    @Override
    public TListTableStatusResult listTableStatus(TGetTablesParams params) throws TException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("get list table request: {}", params);
        }
        TListTableStatusResult result = new TListTableStatusResult();
        List<TTableStatus> tablesResult = Lists.newArrayList();
        result.setTables(tablesResult);
        PatternMatcher matcher = null;
        String specifiedTable = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            } catch (PatternMatcherException e) {
                throw new TException("Pattern is in bad format " + params.getPattern());
            }
        }
        if (params.isSetTable()) {
            specifiedTable = params.getTable();
        }
        // database privs should be checked in analysis phrase

        UserIdentity currentUser;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }

        String catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        if (params.isSetCatalog()) {
            catalogName = params.catalog;
        }
        String dbName = getDbNameFromMysqlTableSchema(catalogName, params.db);
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog != null) {
            DatabaseIf db = catalog.getDbNullable(dbName);
            if (db != null) {
                try {
                    List<TableIf> tables;
                    if (!params.isSetType() || params.getType() == null || params.getType().isEmpty()) {
                        tables = db.getTablesIgnoreException();
                    } else {
                        switch (params.getType()) {
                            case "VIEW":
                                tables = db.getViewsOrEmpty();
                                break;
                            default:
                                tables = db.getTablesIgnoreException();
                        }
                    }
                    for (TableIf table : tables) {
                        if (!Env.getCurrentEnv().getAccessManager()
                                .checkTblPriv(currentUser, catalogName, dbName,
                                        table.getName(), PrivPredicate.SHOW)) {
                            continue;
                        }
                        if (matcher != null && !matcher.match(table.getName())) {
                            continue;
                        }
                        if (specifiedTable != null && !specifiedTable.equals(table.getName())) {
                            continue;
                        }
                        // For the follower node in cloud mode,
                        // when querying the information_schema table,
                        // the version needs to be updated.
                        // Otherwise, the version will always be the old value
                        // unless there is a query for the table in the follower node.
                        if (!Env.getCurrentEnv().isMaster() && Config.isCloudMode()
                                && table instanceof OlapTable) {
                            OlapTable olapTable = (OlapTable) table;
                            List<CloudPartition> partitions = olapTable.getAllPartitions().stream()
                                    .filter(p -> p instanceof CloudPartition)
                                    .map(cloudPartition -> (CloudPartition) cloudPartition)
                                    .collect(Collectors.toList());
                            CloudPartition.getSnapshotVisibleVersion(partitions);
                        }
                        table.readLock();
                        try {
                            long lastCheckTime = table.getLastCheckTime() <= 0 ? 0 : table.getLastCheckTime();
                            TTableStatus status = new TTableStatus();
                            status.setName(table.getName());
                            status.setType(table.getMysqlType());
                            status.setEngine(table.getEngine());
                            status.setComment(table.getComment());
                            status.setCreateTime(table.getCreateTime());
                            status.setLastCheckTime(lastCheckTime / 1000);
                            status.setUpdateTime(table.getUpdateTime() / 1000);
                            status.setCheckTime(lastCheckTime / 1000);
                            status.setCollation("utf-8");
                            status.setRows(table.getCachedRowCount());
                            status.setDataLength(table.getDataLength());
                            status.setAvgRowLength(table.getAvgRowLength());
                            status.setIndexLength(table.getIndexLength());
                            if (table instanceof View) {
                                status.setDdlSql(((View) table).getInlineViewDef());
                            }
                            tablesResult.add(status);
                        } finally {
                            table.readUnlock();
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("failed to get tables for db {} in catalog {}", db.getFullName(), catalogName, e);
                }
            }
        }
        return result;
    }

    public TListTableMetadataNameIdsResult listTableMetadataNameIds(TGetTablesParams params) throws TException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("get list simple table request: {}", params);
        }

        TListTableMetadataNameIdsResult result = new TListTableMetadataNameIdsResult();
        List<TTableMetadataNameIds> tablesResult = Lists.newArrayList();
        result.setTables(tablesResult);

        UserIdentity currentUser;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }

        String catalogName;
        if (params.isSetCatalog()) {
            catalogName = params.catalog;
        } else {
            catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        }

        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            } catch (PatternMatcherException e) {
                throw new TException("Pattern is in bad format " + params.getPattern());
            }
        }
        PatternMatcher finalMatcher = matcher;

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(() -> {

            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
            if (catalog != null) {
                String dbName = getDbNameFromMysqlTableSchema(catalogName, params.db);
                DatabaseIf db = catalog.getDbNullable(dbName);
                if (db != null) {
                    List<TableIf> tables = db.getTables();
                    for (TableIf table : tables) {
                        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUser, catalogName, dbName,
                                table.getName(), PrivPredicate.SHOW)) {
                            continue;
                        }
                        table.readLock();
                        try {
                            if (finalMatcher != null && !finalMatcher.match(table.getName())) {
                                continue;
                            }
                            TTableMetadataNameIds status = new TTableMetadataNameIds();
                            status.setName(table.getName());
                            status.setId(table.getId());

                            tablesResult.add(status);
                        } finally {
                            table.readUnlock();
                        }
                    }
                }
            }
        });
        try {
            if (catalogName.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
                future.get();
            } else {
                future.get(Config.query_metadata_name_ids_timeout, TimeUnit.SECONDS);
            }
        } catch (TimeoutException e) {
            future.cancel(true);
            LOG.info("From catalog:{},db:{} get tables timeout.", catalogName, params.db);
        } catch (InterruptedException | ExecutionException e) {
            future.cancel(true);
        } finally {
            executor.shutdown();
        }
        return result;
    }

    @Override
    public TListPrivilegesResult listTablePrivilegeStatus(TGetTablesParams params) throws TException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("get list table privileges request: {}", params);
        }
        TListPrivilegesResult result = new TListPrivilegesResult();
        List<TPrivilegeStatus> tblPrivResult = Lists.newArrayList();
        result.setPrivileges(tblPrivResult);

        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        Env.getCurrentEnv().getAuth().getTablePrivStatus(tblPrivResult, currentUser);
        return result;
    }

    @Override
    public TListPrivilegesResult listSchemaPrivilegeStatus(TGetTablesParams params) throws TException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("get list schema privileges request: {}", params);
        }
        TListPrivilegesResult result = new TListPrivilegesResult();
        List<TPrivilegeStatus> tblPrivResult = Lists.newArrayList();
        result.setPrivileges(tblPrivResult);

        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        Env.getCurrentEnv().getAuth().getSchemaPrivStatus(tblPrivResult, currentUser);
        return result;
    }

    @Override
    public TListPrivilegesResult listUserPrivilegeStatus(TGetTablesParams params) throws TException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("get list user privileges request: {}", params);
        }
        TListPrivilegesResult result = new TListPrivilegesResult();
        List<TPrivilegeStatus> userPrivResult = Lists.newArrayList();
        result.setPrivileges(userPrivResult);

        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        Env.getCurrentEnv().getAuth().getGlobalPrivStatus(userPrivResult, currentUser);
        return result;
    }

    @Override
    public TFeResult updateExportTaskStatus(TUpdateExportTaskStatusRequest request) throws TException {
        TStatus status = new TStatus(TStatusCode.OK);
        TFeResult result = new TFeResult(FrontendServiceVersion.V1, status);
        return result;
    }

    @Override
    public TDescribeTablesResult describeTables(TDescribeTablesParams params) throws TException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("get desc tables request: {}", params);
        }
        TDescribeTablesResult result = new TDescribeTablesResult();
        List<TColumnDef> columns = Lists.newArrayList();
        List<Integer> tablesOffset = Lists.newArrayList();
        List<String> tables = params.getTablesName();
        result.setColumns(columns);
        result.setTablesOffset(tablesOffset);

        // database privs should be checked in analysis phrase
        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        String dbName = getDbNameFromMysqlTableSchema(params.catalog, params.db);
        for (String tableName : tables) {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(currentUser, params.catalog, dbName, tableName, PrivPredicate.SHOW)) {
                return result;
            }
        }

        String catalogName = Strings.isNullOrEmpty(params.catalog) ? InternalCatalog.INTERNAL_CATALOG_NAME
                : params.catalog;
        DatabaseIf<TableIf> db = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrException(catalogName, catalog -> new TException("Unknown catalog " + catalog))
                .getDbNullable(dbName);
        if (db != null) {
            for (String tableName : tables) {
                TableIf table = db.getTableNullableIfException(tableName);
                if (table != null) {
                    table.readLock();
                    try {
                        List<Column> baseSchema = table.getBaseSchemaOrEmpty();
                        for (Column column : baseSchema) {
                            final TColumnDesc desc = getColumnDesc(column);
                            final TColumnDef colDef = new TColumnDef(desc);
                            final String comment = column.getComment();
                            if (comment != null) {
                                if (Config.column_comment_length_limit > 0
                                        && comment.length() > Config.column_comment_length_limit) {
                                    colDef.setComment(comment.substring(0, Config.column_comment_length_limit));
                                } else {
                                    colDef.setComment(comment);
                                }
                            }
                            if (column.isKey()) {
                                if (table instanceof OlapTable) {
                                    desc.setColumnKey(((OlapTable) table).getKeysType().toMetadata());
                                }
                            }
                            columns.add(colDef);
                        }
                    } finally {
                        table.readUnlock();
                    }
                    tablesOffset.add(columns.size());
                }
            }
        }
        return result;
    }

    private TColumnDesc getColumnDesc(Column column) {
        final TColumnDesc desc = new TColumnDesc(column.getName(), column.getDataType().toThrift());
        final Integer precision = column.getOriginType().getPrecision();
        if (precision != null) {
            desc.setColumnPrecision(precision);
        }
        final Integer columnLength = column.getOriginType().getColumnSize();
        if (columnLength != null) {
            desc.setColumnLength(columnLength);
        }
        final Integer decimalDigits = column.getOriginType().getDecimalDigits();
        if (decimalDigits != null) {
            desc.setColumnScale(decimalDigits);
        }
        desc.setIsAllowNull(column.isAllowNull());
        if (column.getChildren().size() > 0) {
            ArrayList<TColumnDesc> children = new ArrayList<>();
            for (Column child : column.getChildren()) {
                children.add(getColumnDesc(child));
            }
            desc.setChildren(children);
        }
        String defaultValue = column.getDefaultValue();
        if (defaultValue != null) {
            desc.setDefaultValue(defaultValue);
        }
        return desc;
    }

    @Override
    public TShowVariableResult showVariables(TShowVariableRequest params) throws TException {
        TShowVariableResult result = new TShowVariableResult();
        List<List<String>> vars = Lists.newArrayList();
        result.setVariables(vars);
        // Find connect
        ConnectContext ctx = exeEnv.getScheduler().getContext((int) params.getThreadId());
        if (ctx == null) {
            return result;
        }
        vars = VariableMgr.dump(SetType.fromThrift(params.getVarType()), ctx.getSessionVariable(), null);
        result.setVariables(vars);
        return result;
    }

    @Override
    public TReportExecStatusResult reportExecStatus(TReportExecStatusParams params) throws TException {
        return QeProcessorImpl.INSTANCE.reportExecStatus(params, getClientAddr());
    }

    @Override
    public TFetchSplitBatchResult fetchSplitBatch(TFetchSplitBatchRequest request) throws TException {
        TFetchSplitBatchResult result = new TFetchSplitBatchResult();
        SplitSource splitSource =
                Env.getCurrentEnv().getSplitSourceManager().getSplitSource(request.getSplitSourceId());
        if (splitSource == null) {
            throw new TException("Split source " + request.getSplitSourceId() + " is released");
        }
        try {
            List<TScanRangeLocations> locations = splitSource.getNextBatch(request.getMaxNumSplits());
            result.setSplits(locations);
            result.status = new TStatus(TStatusCode.OK);
            return result;
        } catch (Exception e) {
            LOG.warn("failed to fetch split batch with source id {}", request.getSplitSourceId(), e);
            result.status = new TStatus(TStatusCode.INTERNAL_ERROR);
            result.status.addToErrorMsgs(e.getMessage());
        }
        return result;
    }

    @Override
    public TStatus updatePartitionStatsCache(TUpdateFollowerPartitionStatsCacheRequest request) {
        UpdatePartitionStatsTarget target = GsonUtils.GSON.fromJson(request.key, UpdatePartitionStatsTarget.class);
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        analysisManager.updateLocalPartitionStatsCache(target.catalogId, target.dbId, target.tableId,
                target.indexId, target.partitions, target.columnName);
        return new TStatus(TStatusCode.OK);
    }

    @Override
    public TMasterResult finishTask(TFinishTaskRequest request) throws TException {
        return masterImpl.finishTask(request);
    }

    @Override
    public TMasterResult report(TReportRequest request) throws TException {
        return masterImpl.report(request);
    }

    // This interface is used for keeping backward compatible
    @Override
    public TFetchResourceResult fetchResource() throws TException {
        throw new TException("not supported");
    }

    @Override
    public TMasterOpResult forward(TMasterOpRequest params) throws TException {
        Frontend fe = Env.getCurrentEnv().checkFeExist(params.getClientNodeHost(), params.getClientNodePort());
        if (fe == null) {
            LOG.warn("reject request from invalid host. client: {}", params.getClientNodeHost());
            throw new TException("request from invalid host was rejected.");
        }
        if (params.isSyncJournalOnly()) {
            final TMasterOpResult result = new TMasterOpResult();
            result.setMaxJournalId(Env.getCurrentEnv().getMaxJournalId());
            // just make the protocol happy
            result.setPacket("".getBytes());
            return result;
        }
        if (params.getGroupCommitInfo() != null && params.getGroupCommitInfo().isGetGroupCommitLoadBeId()) {
            final TGroupCommitInfo info = params.getGroupCommitInfo();
            final TMasterOpResult result = new TMasterOpResult();
            try {
                result.setGroupCommitLoadBeId(Env.getCurrentEnv().getGroupCommitManager()
                        .selectBackendForGroupCommitInternal(info.groupCommitLoadTableId, info.cluster));
            } catch (LoadException | DdlException e) {
                throw new TException(e.getMessage());
            }
            // just make the protocol happy
            result.setPacket("".getBytes());
            return result;
        }
        if (params.getGroupCommitInfo() != null && params.getGroupCommitInfo().isUpdateLoadData()) {
            final TGroupCommitInfo info = params.getGroupCommitInfo();
            final TMasterOpResult result = new TMasterOpResult();
            Env.getCurrentEnv().getGroupCommitManager()
                    .updateLoadData(info.tableId, info.receiveData);
            // just make the protocol happy
            result.setPacket("".getBytes());
            return result;
        }
        if (params.isSetCancelQeury() && params.isCancelQeury()) {
            if (!params.isSetQueryId()) {
                throw new TException("a query id is needed to cancel a query");
            }
            TUniqueId queryId = params.getQueryId();
            ConnectContext ctx = proxyQueryIdToConnCtx.get(queryId);
            if (ctx != null) {
                ctx.cancelQuery(new Status(TStatusCode.CANCELLED, "cancel query by forward request."));
            }
            final TMasterOpResult result = new TMasterOpResult();
            result.setStatusCode(0);
            result.setMaxJournalId(Env.getCurrentEnv().getMaxJournalId());
            // just make the protocol happy
            result.setPacket("".getBytes());
            return result;
        }

        // add this log so that we can track this stmt
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive forwarded stmt {} from FE: {}", params.getStmtId(), params.getClientNodeHost());
        }
        ConnectContext context = new ConnectContext(null, true, params.getSessionId());
        // Set current connected FE to the client address, so that we can know where
        // this request come from.
        context.setCurrentConnectedFEIp(params.getClientNodeHost());
        if (Config.isCloudMode() && !Strings.isNullOrEmpty(params.getCloudCluster())) {
            context.setCloudCluster(params.getCloudCluster());
        }

        ConnectProcessor processor = null;
        if (context.getConnectType().equals(ConnectType.MYSQL)) {
            processor = new MysqlConnectProcessor(context);
        } else if (context.getConnectType().equals(ConnectType.ARROW_FLIGHT_SQL)) {
            processor = new FlightSqlConnectProcessor(context);
        } else {
            throw new TException("unknown ConnectType: " + context.getConnectType());
        }
        Runnable clearCallback = () -> {};
        if (params.isSetQueryId()) {
            proxyQueryIdToConnCtx.put(params.getQueryId(), context);
            clearCallback = () -> proxyQueryIdToConnCtx.remove(params.getQueryId());
        }
        TMasterOpResult result = processor.proxyExecute(params);
        if (QueryState.MysqlStateType.ERR.name().equalsIgnoreCase(result.getStatus())) {
            context.getState().setError(result.getStatus());
        } else {
            context.getState().setOk();
        }
        ConnectContext.remove();
        clearCallback.run();
        return result;
    }

    private List<String> getTableNames(String dbName, List<Long> tableIds) throws UserException {
        final String fullDbName = dbName;
        Database db = Env.getCurrentInternalCatalog().getDbNullable(fullDbName);
        if (db == null) {
            throw new UserException(String.format("can't find db named: %s", dbName));
        }
        List<String> tableNames = Lists.newArrayList();
        for (Long id : tableIds) {
            Table table = db.getTableNullable(id);
            if (table == null) {
                throw new UserException(String.format("can't find table id: %d in db: %s", id, dbName));
            }
            tableNames.add(table.getName());
        }

        return tableNames;
    }

    private void checkSingleTablePasswordAndPrivs(String user, String passwd, String db, String tbl,
            String clientIp, PrivPredicate predicate) throws AuthenticationException {
        checkPasswordAndPrivs(user, passwd, db, Lists.newArrayList(tbl), clientIp, predicate);
    }

    private void checkDbPasswordAndPrivs(String user, String passwd, String db, String clientIp,
            PrivPredicate predicate) throws AuthenticationException {
        checkPasswordAndPrivs(user, passwd, db, null, clientIp, predicate);
    }

    private void checkPasswordAndPrivs(String user, String passwd, String db, List<String> tables,
            String clientIp, PrivPredicate predicate) throws AuthenticationException {

        final String fullUserName = ClusterNamespace.getNameFromFullName(user);
        final String fullDbName = db;
        List<UserIdentity> currentUser = Lists.newArrayList();
        Env.getCurrentEnv().getAuth().checkPlainPassword(fullUserName, clientIp, passwd, currentUser);

        Preconditions.checkState(currentUser.size() == 1);
        if (tables == null || tables.isEmpty()) {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(currentUser.get(0), InternalCatalog.INTERNAL_CATALOG_NAME, fullDbName, predicate)) {
                throw new AuthenticationException(
                        "Access denied; you need (at least one of) the (" + predicate.toString()
                                + ") privilege(s) for this operation");
            }
            return;
        }

        for (String tbl : tables) {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(currentUser.get(0), InternalCatalog.INTERNAL_CATALOG_NAME, fullDbName, tbl,
                            predicate)) {
                throw new AuthenticationException(
                        "Access denied; you need (at least one of) the (" + predicate.toString()
                                + ") privilege(s) for this operation");
            }
        }
    }

    private void checkPassword(String user, String passwd, String clientIp)
            throws AuthenticationException {
        final String fullUserName = ClusterNamespace.getNameFromFullName(user);
        List<UserIdentity> currentUser = Lists.newArrayList();
        Env.getCurrentEnv().getAuth().checkPlainPassword(fullUserName, clientIp, passwd, currentUser);
        Preconditions.checkState(currentUser.size() == 1);
    }

    @Override
    public TLoadTxnBeginResult loadTxnBegin(TLoadTxnBeginRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive txn begin request: {}, backend: {}", request, clientAddr);
        }

        TLoadTxnBeginResult result = new TLoadTxnBeginResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to loadTxnBegin:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, clientAddr);
            return result;
        }

        try {
            TLoadTxnBeginResult tmpRes = loadTxnBeginImpl(request, clientAddr);
            result.setTxnId(tmpRes.getTxnId()).setDbId(tmpRes.getDbId());
        } catch (DuplicatedRequestException e) {
            // this is a duplicate request, just return previous txn id
            LOG.warn("duplicate request for stream load. request id: {}, txn: {}", e.getDuplicatedRequestId(),
                    e.getTxnId());
            result.setTxnId(e.getTxnId());
        } catch (LabelAlreadyUsedException e) {
            status.setStatusCode(TStatusCode.LABEL_ALREADY_EXISTS);
            status.addToErrorMsgs(e.getMessage());
            result.setJobStatus(e.getJobStatus());
        } catch (MetaNotFoundException e) {
            LOG.warn("failed to begin", e);
            status.setStatusCode(TStatusCode.NOT_FOUND);
            status.addToErrorMsgs(e.getMessage());
        } catch (UserException e) {
            LOG.warn("failed to begin: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    private TLoadTxnBeginResult loadTxnBeginImpl(TLoadTxnBeginRequest request, String clientIp) throws UserException {
        if (request.isSetAuthCode()) {
            // TODO: deprecated, removed in 3.1, use token instead.
        } else if (Strings.isNullOrEmpty(request.getToken())) {
            checkSingleTablePasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(),
                    request.getUserIp(), PrivPredicate.LOAD);
        } else {
            if (!checkToken(request.getToken())) {
                throw new AuthenticationException("Invalid token: " + request.getToken());
            }
        }

        // check label
        if (Strings.isNullOrEmpty(request.getLabel())) {
            throw new UserException("empty label in begin request");
        }
        // check database
        Env env = Env.getCurrentEnv();
        String fullDbName = request.getDb();
        Database db = env.getInternalCatalog().getDbNullable(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new MetaNotFoundException("unknown database, database=" + dbName);
        }

        OlapTable table = (OlapTable) db.getTableOrMetaException(request.tbl, TableType.OLAP);
        // begin
        long timeoutSecond = request.isSetTimeout() ? request.getTimeout() : Config.stream_load_default_timeout_second;
        Backend backend = Env.getCurrentSystemInfo().getBackend(request.getBackendId());
        long startTime = backend != null ? backend.getLastStartTime() : 0;
        TxnCoordinator txnCoord = new TxnCoordinator(TxnSourceType.BE, request.getBackendId(), clientIp, startTime);
        if (request.isSetToken()) {
            txnCoord.isFromInternal = true;
        }
        long txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                db.getId(), Lists.newArrayList(table.getId()), request.getLabel(), request.getRequestId(),
                txnCoord,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, -1, timeoutSecond);
        TLoadTxnBeginResult result = new TLoadTxnBeginResult();
        result.setTxnId(txnId).setDbId(db.getId());
        return result;
    }

    @Override
    public TBeginTxnResult beginTxn(TBeginTxnRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive txn begin request: {}, client: {}", request, clientAddr);
        }

        TBeginTxnResult result = new TBeginTxnResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get beginTxn: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        try {
            TBeginTxnResult tmpRes = beginTxnImpl(request, clientAddr);
            result.setTxnId(tmpRes.getTxnId()).setDbId(tmpRes.getDbId());
            if (tmpRes.isSetSubTxnIds()) {
                result.setSubTxnIds(tmpRes.getSubTxnIds());
            }
        } catch (DuplicatedRequestException e) {
            // this is a duplicate request, just return previous txn id
            LOG.warn("duplicate request for stream load. request id: {}, txn: {}", e.getDuplicatedRequestId(),
                    e.getTxnId());
            result.setTxnId(e.getTxnId());
        } catch (LabelAlreadyUsedException e) {
            status.setStatusCode(TStatusCode.LABEL_ALREADY_EXISTS);
            status.addToErrorMsgs(e.getMessage());
            result.setJobStatus(e.getJobStatus());
        } catch (UserException e) {
            LOG.warn("failed to begin: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    private TBeginTxnResult beginTxnImpl(TBeginTxnRequest request, String clientIp) throws UserException {
        /// Check required arg: user, passwd, db, tables, label
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }
        if (!request.isSetTableIds()) {
            throw new UserException("table ids is not set");
        }
        if (!request.isSetLabel()) {
            throw new UserException("label is not set");
        }

        // step 1: check auth
        if (Strings.isNullOrEmpty(request.getToken())) {
            // lookup table ids && convert into tableNameList
            List<String> tableNameList = getTableNames(request.getDb(), request.getTableIds());
            checkPasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(), tableNameList,
                    request.getUserIp(), PrivPredicate.LOAD);
        }

        // step 2: check label
        if (Strings.isNullOrEmpty(request.getLabel())) {
            throw new UserException("empty label in begin request");
        }

        // step 3: check database
        Env env = Env.getCurrentEnv();
        String fullDbName = request.getDb();
        Database db = env.getInternalCatalog().getDbNullable(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new MetaNotFoundException("unknown database, database=" + dbName);
        }

        // step 4: fetch all tableIds
        // table ids is checked at step 1
        List<Long> tableIdList = request.getTableIds();

        // step 5: get timeout
        long timeoutSecond = request.isSetTimeout() ? request.getTimeout() : Config.stream_load_default_timeout_second;

        Backend backend = Env.getCurrentSystemInfo().getBackend(request.getBackendId());
        long startTime = backend != null ? backend.getLastStartTime() : 0;
        TxnCoordinator txnCoord = new TxnCoordinator(TxnSourceType.BE, request.getBackendId(), clientIp, startTime);
        // step 6: begin transaction
        long txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                db.getId(), tableIdList, request.getLabel(), request.getRequestId(), txnCoord,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, -1, timeoutSecond);

        // step 7: return result
        TBeginTxnResult result = new TBeginTxnResult();
        result.setTxnId(txnId).setDbId(db.getId());
        if (request.isSetSubTxnNum() && request.getSubTxnNum() > 0) {
            result.addToSubTxnIds(txnId);
            for (int i = 0; i < request.getSubTxnNum() - 1; i++) {
                result.addToSubTxnIds(Env.getCurrentGlobalTransactionMgr().getNextTransactionId());
            }
        }
        return result;
    }

    @Override
    public TLoadTxnCommitResult loadTxnPreCommit(TLoadTxnCommitRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive txn pre-commit request: {}, backend: {}", request, clientAddr);
        }

        TLoadTxnCommitResult result = new TLoadTxnCommitResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to loadTxnPreCommit:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, clientAddr);
            return result;
        }
        try {
            loadTxnPreCommitImpl(request);
        } catch (UserException e) {
            LOG.warn("failed to pre-commit txn: {}: {}", request.getTxnId(), e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    private List<Table> queryLoadCommitTables(TLoadTxnCommitRequest request, Database db) throws UserException {
        if (request.isSetTableId() && request.getTableId() > 0) {
            Table table = Env.getCurrentEnv().getInternalCatalog().getTableByTableId(request.getTableId());
            if (table == null) {
                throw new MetaNotFoundException("unknown table, table_id=" + request.getTableId());
            }
            return Collections.singletonList(table);
        }

        List<String> tbNames;
        // check has multi table
        if (CollectionUtils.isNotEmpty(request.getTbls())) {
            tbNames = request.getTbls();
        } else {
            tbNames = Collections.singletonList(request.getTbl());
        }
        List<Table> tables = new ArrayList<>(tbNames.size());
        for (String tbl : tbNames) {
            OlapTable table = (OlapTable) db.getTableOrMetaException(tbl, TableType.OLAP);
            tables.add(table);
        }
        if (tables.size() > 1) {
            tables.sort(Comparator.comparing(Table::getId));
        }
        // if it has multi table, use multi table and update multi table running
        // transaction table ids
        if (CollectionUtils.isNotEmpty(request.getTbls())) {
            List<Long> multiTableIds = tables.stream().map(Table::getId).collect(Collectors.toList());
            Env.getCurrentGlobalTransactionMgr()
                    .updateMultiTableRunningTransactionTableIds(db.getId(), request.getTxnId(), multiTableIds);
            if (LOG.isDebugEnabled()) {
                LOG.debug("txn {} has multi table {}", request.getTxnId(), request.getTbls());
            }
        }
        return tables;
    }

    private void loadTxnPreCommitImpl(TLoadTxnCommitRequest request) throws UserException {
        if (request.isSetAuthCode()) {
            // TODO: deprecated, removed in 3.1, use token instead.
        } else if (request.isSetToken()) {
            if (!checkToken(request.getToken())) {
                throw new AuthenticationException("Invalid token: " + request.getToken());
            }
        } else {
            // refactoring it
            if (CollectionUtils.isNotEmpty(request.getTbls())) {
                for (String tbl : request.getTbls()) {
                    checkSingleTablePasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(),
                            tbl,
                            request.getUserIp(), PrivPredicate.LOAD);
                }
            } else {
                checkSingleTablePasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(),
                        request.getTbl(),
                        request.getUserIp(), PrivPredicate.LOAD);
            }
        }

        // get database
        Env env = Env.getCurrentEnv();
        String fullDbName = request.getDb();
        Database db;
        if (request.isSetDbId() && request.getDbId() > 0) {
            db = env.getInternalCatalog().getDbNullable(request.getDbId());
        } else {
            db = env.getInternalCatalog().getDbNullable(fullDbName);
        }
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() / 2 : 5000;
        List<Table> tables = queryLoadCommitTables(request, db);
        Env.getCurrentGlobalTransactionMgr()
                .preCommitTransaction2PC(db, tables, request.getTxnId(),
                        TabletCommitInfo.fromThrift(request.getCommitInfos()), timeoutMs,
                        TxnCommitAttachment.fromThrift(request.txnCommitAttachment));
    }

    @Override
    public TLoadTxn2PCResult loadTxn2PC(TLoadTxn2PCRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive txn 2PC request: {}, backend: {}", request, clientAddr);
        }

        TLoadTxn2PCResult result = new TLoadTxn2PCResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to loadTxn2PC:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, clientAddr);
            return result;
        }

        try {
            loadTxn2PCImpl(request);
        } catch (UserException e) {
            LOG.warn("failed to {} txn {}: {}", request.getOperation(), request.getTxnId(), e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    private void loadTxn2PCImpl(TLoadTxn2PCRequest request) throws UserException {
        String dbName = request.getDb();
        if (Strings.isNullOrEmpty(dbName)) {
            throw new UserException("No database selected.");
        }

        String fullDbName = dbName;

        // get database
        Env env = Env.getCurrentEnv();
        Database database = env.getInternalCatalog().getDbNullable(fullDbName);
        if (database == null) {
            throw new UserException("unknown database, database=" + fullDbName);
        }

        String txnOperation = request.getOperation().trim();
        if (!request.isSetTxnId()) {
            List<TransactionStatus> statusList = new ArrayList<>();
            statusList.add(TransactionStatus.PRECOMMITTED);
            if (txnOperation.equalsIgnoreCase("abort")) {
                statusList.add(TransactionStatus.PREPARE);
            }
            request.setTxnId(Env.getCurrentGlobalTransactionMgr()
                    .getTransactionIdByLabel(database.getId(), request.getLabel(), statusList));
        }
        TransactionState transactionState = Env.getCurrentGlobalTransactionMgr()
                .getTransactionState(database.getId(), request.getTxnId());
        if (transactionState == null) {
            throw new UserException("transaction [" + request.getTxnId() + "] not found");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("txn {} has multi table {}", request.getTxnId(), transactionState.getTableIdList());
        }
        List<Long> tableIdList = transactionState.getTableIdList();
        List<Table> tableList = new ArrayList<>();
        // if table was dropped, stream load must can abort.
        if (txnOperation.equalsIgnoreCase("abort")) {
            tableList = database.getTablesOnIdOrderIfExist(tableIdList);
        } else {
            tableList = database.getTablesOnIdOrderOrThrowException(tableIdList);
        }
        for (Table table : tableList) {
            // check auth
            checkSingleTablePasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(),
                    table.getName(),
                    request.getUserIp(), PrivPredicate.LOAD);
        }

        if (txnOperation.equalsIgnoreCase("commit")) {
            long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() / 2 : 5000;
            Env.getCurrentGlobalTransactionMgr()
                    .commitTransaction2PC(database, tableList, request.getTxnId(), timeoutMs);
        } else if (txnOperation.equalsIgnoreCase("abort")) {
            Env.getCurrentGlobalTransactionMgr().abortTransaction2PC(database.getId(), request.getTxnId(), tableList);
        } else {
            throw new UserException("transaction operation should be \'commit\' or \'abort\'");
        }
    }

    @Override
    public TLoadTxnCommitResult loadTxnCommit(TLoadTxnCommitRequest request) throws TException {
        multiTableFragmentInstanceIdIndexMap.remove(request.getTxnId());
        deleteMultiTableStreamLoadJobIndex(request.getTxnId());
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive txn commit request: {}, backend: {}", request, clientAddr);
        }

        TLoadTxnCommitResult result = new TLoadTxnCommitResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to loadTxnCommit:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, clientAddr);
            return result;
        }

        if (DebugPointUtil.isEnable("load.commit_timeout")) {
            try {
                Thread.sleep(60 * 1000);
            } catch (InterruptedException e) {
                LOG.warn("failed to sleep", e);
            }
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs("load commit timeout");
            return result;
        }

        try {
            if (!loadTxnCommitImpl(request)) {
                // committed success but not visible
                status.setStatusCode(TStatusCode.PUBLISH_TIMEOUT);
                status.addToErrorMsgs("transaction commit successfully, BUT data will be visible later");
            }
        } catch (UserException e) {
            LOG.warn("failed to commit txn: {}", request.getTxnId(), e);
            // DELETE_BITMAP_LOCK_ERR will be retried on be
            if (e.getErrorCode() == InternalErrorCode.DELETE_BITMAP_LOCK_ERR) {
                status.setStatusCode(TStatusCode.DELETE_BITMAP_LOCK_ERROR);
                status.addToErrorMsgs(e.getMessage());
            } else {
                status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
                status.addToErrorMsgs(e.getMessage());
            }
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    // return true if commit success and publish success, return false if publish
    // timeout
    private boolean loadTxnCommitImpl(TLoadTxnCommitRequest request) throws UserException {
        if (request.isSetAuthCode()) {
            // TODO: deprecated, removed in 3.1, use token instead.
        } else if (request.isSetToken()) {
            checkToken(request.getToken());
        } else {
            if (CollectionUtils.isNotEmpty(request.getTbls())) {
                checkPasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(),
                        request.getTbls(), request.getUserIp(), PrivPredicate.LOAD);
            } else {
                checkSingleTablePasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(),
                        request.getTbl(), request.getUserIp(), PrivPredicate.LOAD);
            }
        }
        if (request.groupCommit) {
            try {
                Env.getCurrentEnv().getGroupCommitManager().updateLoadData(request.table_id, request.receiveBytes);
            } catch (Exception e) {
                LOG.warn("Failed to update group commit load data, {}", e.getMessage());
            }
        }

        // get database
        Env env = Env.getCurrentEnv();
        String fullDbName = request.getDb();
        Database db;
        if (request.isSetDbId() && request.getDbId() > 0) {
            db = env.getInternalCatalog().getDbNullable(request.getDbId());
        } else {
            db = env.getInternalCatalog().getDbNullable(fullDbName);
        }
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName + " request.isSetDbId(): "
                    + request.isSetDbId() + " id: " + Long.toString(request.isSetDbId() ? request.getDbId() : 0)
                    + " fullDbName: " + fullDbName);
        }
        long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() / 2
                : Config.try_commit_lock_timeout_seconds * 1000;
        List<Table> tables = queryLoadCommitTables(request, db);
        return Env.getCurrentGlobalTransactionMgr()
                .commitAndPublishTransaction(db, tables, request.getTxnId(),
                        TabletCommitInfo.fromThrift(request.getCommitInfos()), timeoutMs,
                        TxnCommitAttachment.fromThrift(request.txnCommitAttachment));
    }

    @Override
    public TCommitTxnResult commitTxn(TCommitTxnRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive txn commit request: {}, client: {}", request, clientAddr);
        }

        TCommitTxnResult result = new TCommitTxnResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get commitTxn: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        try {
            if (!commitTxnImpl(request)) {
                // committed success but not visible
                status.setStatusCode(TStatusCode.PUBLISH_TIMEOUT);
                status.addToErrorMsgs("transaction commit successfully, BUT data will be visible later");
            }
        } catch (UserException e) {
            LOG.warn("failed to commit txn: {}: {}", request.getTxnId(), e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    // return true if commit success and publish success, return false if publish
    // timeout
    private boolean commitTxnImpl(TCommitTxnRequest request) throws UserException {
        /// Check required arg: user, passwd, db, txn_id, commit_infos
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }
        if (!request.isSetTxnId()) {
            throw new UserException("txn_id is not set");
        }
        if (request.isSetTxnInsert() && request.isTxnInsert()) {
            if (!request.isSetSubTxnInfos()) {
                throw new UserException("sub_txn_infos is not set");
            }
        } else {
            if (!request.isSetCommitInfos()) {
                throw new UserException("commit_infos is not set");
            }
        }

        // Step 1: get && check database
        Env env = Env.getCurrentEnv();
        String fullDbName = request.getDb();
        Database db;
        if (request.isSetDbId() && request.getDbId() > 0) {
            db = env.getInternalCatalog().getDbNullable(request.getDbId());
        } else {
            db = env.getInternalCatalog().getDbNullable(fullDbName);
        }
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        // Step 2: get tables
        TransactionState transactionState = Env.getCurrentGlobalTransactionMgr()
                .getTransactionState(db.getId(), request.getTxnId());

        if (transactionState == null) {
            throw new UserException("transaction [" + request.getTxnId() + "] not found");
        }
        List<Long> tableIdList = transactionState.getTableIdList();
        // if table was dropped, transaction must be aborted
        List<Table> tableList = db.getTablesOnIdOrderOrThrowException(tableIdList);

        // Step 3: check auth
        if (request.isSetAuthCode()) {
            // TODO: deprecated, removed in 3.1, use token instead.
        } else if (request.isSetToken()) {
            checkToken(request.getToken());
        } else {
            List<String> tables = tableList.stream().map(Table::getName).collect(Collectors.toList());
            checkPasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(), tables,
                    request.getUserIp(), PrivPredicate.LOAD);
        }

        // Step 4: get timeout
        long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() / 2
                : Config.try_commit_lock_timeout_seconds * 1000;

        // Step 5: commit and publish
        if (request.isSetTxnInsert() && request.isTxnInsert()) {
            List<Long> subTxnIds = new ArrayList<>();
            List<SubTransactionState> subTransactionStates = new ArrayList<>();
            for (TSubTxnInfo subTxnInfo : request.getSubTxnInfos()) {
                TableIf table = db.getTableNullable(subTxnInfo.getTableId());
                if (table == null) {
                    continue;
                }
                subTxnIds.add(subTxnInfo.getSubTxnId());
                subTransactionStates.add(
                        new SubTransactionState(subTxnInfo.getSubTxnId(), (Table) table,
                                subTxnInfo.getTabletCommitInfos(), null));
            }
            transactionState.setSubTxnIds(subTxnIds);
            return Env.getCurrentGlobalTransactionMgr()
                    .commitAndPublishTransaction(db, request.getTxnId(),
                            subTransactionStates, timeoutMs);
        } else if (!request.isOnlyCommit()) {
            return Env.getCurrentGlobalTransactionMgr()
                    .commitAndPublishTransaction(db, tableList,
                            request.getTxnId(),
                            TabletCommitInfo.fromThrift(request.getCommitInfos()), timeoutMs,
                            TxnCommitAttachment.fromThrift(request.getTxnCommitAttachment()));
        } else {
            // single table commit, so don't need to wait for publish.
            Env.getCurrentGlobalTransactionMgr()
                    .commitTransaction(db, tableList,
                            request.getTxnId(),
                            TabletCommitInfo.fromThrift(request.getCommitInfos()), timeoutMs,
                            TxnCommitAttachment.fromThrift(request.getTxnCommitAttachment()));
            return true;
        }
    }

    @Override
    public TLoadTxnRollbackResult loadTxnRollback(TLoadTxnRollbackRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive txn rollback request: {}, backend: {}", request, clientAddr);
        }
        TLoadTxnRollbackResult result = new TLoadTxnRollbackResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to loadTxnRollback:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, clientAddr);
            return result;
        }
        try {
            if (DebugPointUtil.isEnable("FrontendServiceImpl.loadTxnRollback.error")) {
                throw new UserException("FrontendServiceImpl.loadTxnRollback.error");
            }
            loadTxnRollbackImpl(request);
        } catch (MetaNotFoundException e) {
            LOG.warn("failed to rollback txn, id: {}, label: {}", request.getTxnId(), request.getLabel(), e);
            status.setStatusCode(TStatusCode.NOT_FOUND);
            status.addToErrorMsgs(e.getMessage());
        } catch (UserException e) {
            LOG.warn("failed to rollback txn, id: {}, label: {}", request.getTxnId(), request.getLabel(), e);
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    private void loadTxnRollbackImpl(TLoadTxnRollbackRequest request) throws UserException {
        if (request.isSetAuthCode()) {
            // TODO: deprecated, removed in 3.1, use token instead.
        } else if (request.isSetToken()) {
            checkToken(request.getToken());
        } else {
            // multi table load
            if (CollectionUtils.isNotEmpty(request.getTbls())) {
                for (String tbl : request.getTbls()) {
                    checkSingleTablePasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(),
                            tbl,
                            request.getUserIp(), PrivPredicate.LOAD);
                }
            } else {
                checkSingleTablePasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(),
                        request.getTbl(),
                        request.getUserIp(), PrivPredicate.LOAD);
            }
        }
        String dbName = request.getDb();
        Database db;
        if (request.isSetDbId() && request.getDbId() > 0) {
            db = Env.getCurrentInternalCatalog().getDbNullable(request.getDbId());
        } else {
            db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
        }
        if (db == null) {
            throw new MetaNotFoundException("db " + request.getDb() + " does not exist");
        }
        long dbId = db.getId();
        if (request.getTxnId() > 0) { // txnId is required in thrift
            TransactionState transactionState = Env.getCurrentGlobalTransactionMgr()
                    .getTransactionState(dbId, request.getTxnId());
            if (transactionState == null) {
                throw new UserException("transaction [" + request.getTxnId() + "] not found");
            }
            List<Table> tableList = db.getTablesOnIdOrderIfExist(transactionState.getTableIdList());
            Env.getCurrentGlobalTransactionMgr().abortTransaction(dbId, request.getTxnId(),
                    request.isSetReason() ? request.getReason() : "system cancel",
                    TxnCommitAttachment.fromThrift(request.getTxnCommitAttachment()), tableList);
        } else if (request.isSetLabel()) {
            Env.getCurrentGlobalTransactionMgr()
                    .abortTransaction(db.getId(), request.getLabel(),
                            request.isSetReason() ? request.getReason() : "system cancel");
        } else {
            throw new UserException("must set txn_id or label");
        }
    }

    @Override
    public TRollbackTxnResult rollbackTxn(TRollbackTxnRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive txn rollback request: {}, client: {}", request, clientAddr);
        }
        TRollbackTxnResult result = new TRollbackTxnResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get rollbackTxn: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        try {
            rollbackTxnImpl(request);
        } catch (UserException e) {
            LOG.warn("failed to rollback txn {}: {}", request.getTxnId(), e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    private void rollbackTxnImpl(TRollbackTxnRequest request) throws UserException {
        /// Check required arg: user, passwd, db, txn_id
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }
        if (!request.isSetTxnId()) {
            throw new UserException("txn_id is not set");
        }

        // Step 1: get && check database
        Env env = Env.getCurrentEnv();
        String fullDbName = request.getDb();
        Database db;
        if (request.isSetDbId() && request.getDbId() > 0) {
            db = env.getInternalCatalog().getDbNullable(request.getDbId());
        } else {
            db = env.getInternalCatalog().getDbNullable(fullDbName);
        }
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        // Step 2: get tables
        TransactionState transactionState = Env.getCurrentGlobalTransactionMgr()
                .getTransactionState(db.getId(), request.getTxnId());
        if (transactionState == null) {
            throw new UserException("transaction [" + request.getTxnId() + "] not found");
        }
        List<Long> tableIdList = transactionState.getTableIdList();
        List<Table> tableList = db.getTablesOnIdOrderOrThrowException(tableIdList);

        // Step 3: check auth
        if (request.isSetAuthCode()) {
            // TODO: deprecated, removed in 3.1, use token instead.
        } else if (request.isSetToken()) {
            checkToken(request.getToken());
        } else {
            List<String> tables = tableList.stream().map(Table::getName).collect(Collectors.toList());
            checkPasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(), tables,
                    request.getUserIp(), PrivPredicate.LOAD);
        }

        // Step 4: abort txn
        Env.getCurrentGlobalTransactionMgr().abortTransaction(db.getId(), request.getTxnId(),
                request.isSetReason() ? request.getReason() : "system cancel",
                TxnCommitAttachment.fromThrift(request.getTxnCommitAttachment()), tableList);
    }

    @Override
    public TStreamLoadPutResult streamLoadPut(TStreamLoadPutRequest request) {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive stream load put request: {}, backend: {}", request, clientAddr);
        }

        TStreamLoadPutResult result = new TStreamLoadPutResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        StreamLoadHandler streamLoadHandler = new StreamLoadHandler(request, null, result, clientAddr);

        try {
            streamLoadHandler.setCloudCluster();

            List<TPipelineWorkloadGroup> tWorkloadGroupList = null;
            // mysql load request not carry user info, need fix it later.
            boolean hasUserName = !StringUtils.isEmpty(request.getUser());
            if (Config.enable_workload_group && hasUserName) {
                tWorkloadGroupList = Env.getCurrentEnv().getWorkloadGroupMgr()
                        .getWorkloadGroup(ConnectContext.get());
            }
            if (!Strings.isNullOrEmpty(request.getLoadSql())) {
                httpStreamPutImpl(request, result);
                if (tWorkloadGroupList != null && tWorkloadGroupList.size() > 0) {
                    result.pipeline_params.setWorkloadGroups(tWorkloadGroupList);
                }
                return result;
            } else {
                streamLoadHandler.generatePlan();
                result.setPipelineParams((TPipelineFragmentParams) streamLoadHandler.getFragmentParams().get(0));
            }
            if (tWorkloadGroupList != null && tWorkloadGroupList.size() > 0) {
                result.pipeline_params.setWorkloadGroups(tWorkloadGroupList);
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("failed to rollback txn, id: {}, label: {}", request.getTxnId(), request.getLabel(), e);
            status.setStatusCode(TStatusCode.NOT_FOUND);
            status.addToErrorMsgs(e.getMessage());
        } catch (UserException e) {
            LOG.warn("failed to get stream load plan, label: {}", request.getLabel(), e);
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("stream load catch unknown result, label: {}", request.getLabel(), e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(e.getClass().getSimpleName() + ": " + Strings.nullToEmpty(e.getMessage()));
            return result;
        } finally {
            ConnectContext.remove();
        }
        return result;
    }

    private void deleteMultiTableStreamLoadJobIndex(long txnId) {
        try {
            Env.getCurrentEnv().getRoutineLoadManager().removeMultiLoadTaskTxnIdToRoutineLoadJobId(txnId);
        } catch (Exception e) {
            LOG.warn("failed to delete multi table stream load job index: {}", e.getMessage());
        }
    }

    @Override
    public TStreamLoadMultiTablePutResult streamLoadMultiTablePut(TStreamLoadPutRequest request) {
        TStreamLoadMultiTablePutResult result = new TStreamLoadMultiTablePutResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        List planFragmentParamsList = new ArrayList<>();
        multiTableFragmentInstanceIdIndexMap.putIfAbsent(request.getTxnId(), new AtomicInteger(1));
        AtomicInteger index = multiTableFragmentInstanceIdIndexMap.get(request.getTxnId());
        StreamLoadHandler streamLoadHandler = new StreamLoadHandler(request, index, null,
                                                                    getClientAddrAsString());
        try {
            streamLoadHandler.generatePlan();
            planFragmentParamsList.addAll(streamLoadHandler.getFragmentParams());
            if (LOG.isDebugEnabled()) {
                LOG.debug("receive stream load multi table put request result: {}", result);
            }
        }  catch (UserException exception) {
            LOG.warn("failed to get stream load plan: {}", exception.getMessage());
            status = new TStatus(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(exception.getMessage());
            result.setStatus(status);
            try {
                RoutineLoadJob routineLoadJob = Env.getCurrentEnv().getRoutineLoadManager()
                        .getRoutineLoadJobByMultiLoadTaskTxnId(request.getTxnId());
                routineLoadJob.updateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.INTERNAL_ERR,
                            "failed to get stream load plan, " + exception.getMessage()), false);
            } catch (Throwable e) {
                LOG.warn("catch update routine load job error.", e);
            }
            return result;
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(e.getClass().getSimpleName() + ": " + Strings.nullToEmpty(e.getMessage()));
            return result;
        } finally {
            ConnectContext.remove();
        }
        result.setPipelineParams(planFragmentParamsList);
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive stream load multi table put request result: {}", result);
        }
        return result;
    }

    private HttpStreamParams initHttpStreamPlan(TStreamLoadPutRequest request, ConnectContext ctx)
            throws UserException {
        String originStmt = request.getLoadSql();
        HttpStreamParams httpStreamParams;
        try {
            while (DebugPointUtil.isEnable("FE.FrontendServiceImpl.initHttpStreamPlan.block")) {
                Thread.sleep(1000);
                LOG.info("block initHttpStreamPlan");
            }
            StmtExecutor executor = new StmtExecutor(ctx, originStmt);
            httpStreamParams = executor.generateHttpStreamPlan(ctx.queryId());

            Analyzer analyzer = new Analyzer(ctx.getEnv(), ctx);
            Coordinator coord = new Coordinator(ctx, analyzer, executor.planner());
            coord.setLoadMemLimit(request.getExecMemLimit());
            coord.setQueryType(TQueryType.LOAD);
            TableIf table = httpStreamParams.getTable();
            if (table instanceof OlapTable) {
                boolean isEnableMemtableOnSinkNode = ((OlapTable) table).getTableProperty().getUseSchemaLightChange()
                        ? coord.getQueryOptions().isEnableMemtableOnSinkNode()
                        : false;
                coord.getQueryOptions().setEnableMemtableOnSinkNode(isEnableMemtableOnSinkNode);
            }
            httpStreamParams.setParams(coord.getStreamLoadPlan());
        } catch (UserException e) {
            LOG.warn("exec sql error", e);
            throw e;
        } catch (Throwable e) {
            LOG.warn("exec sql error catch unknown result.", e);
            throw new UserException("exec sql error catch unknown result." + e);
        }
        return httpStreamParams;
    }

    private void httpStreamPutImpl(TStreamLoadPutRequest request, TStreamLoadPutResult result)
            throws UserException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive http stream put request: {}", request);
        }

        ConnectContext ctx = ConnectContext.get();

        if (request.isSetAuthCode()) {
            // TODO: deprecated, removed in 3.1, use token instead.
        } else if (Strings.isNullOrEmpty(request.getToken())) {
            checkSingleTablePasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(),
                    request.getUserIp(), PrivPredicate.LOAD);
        }
        if (request.isSetMemtableOnSinkNode()) {
            ctx.getSessionVariable().enableMemtableOnSinkNode = request.isMemtableOnSinkNode();
        } else {
            ctx.getSessionVariable().enableMemtableOnSinkNode = Config.stream_load_default_memtable_on_sink_node;
        }
        ctx.getSessionVariable().groupCommit = request.getGroupCommitMode();
        if (request.isSetPartialUpdate() && !request.isPartialUpdate()) {
            ctx.getSessionVariable().setEnableUniqueKeyPartialUpdate(false);
        }
        try {
            HttpStreamParams httpStreamParams = initHttpStreamPlan(request, ctx);
            int loadStreamPerNode = 2;
            if (request.getStreamPerNode() > 0) {
                loadStreamPerNode = request.getStreamPerNode();
            }
            httpStreamParams.getParams().setLoadStreamPerNode(loadStreamPerNode);
            httpStreamParams.getParams().setTotalLoadStreams(loadStreamPerNode);
            httpStreamParams.getParams().setNumLocalSink(1);

            TransactionState txnState = Env.getCurrentGlobalTransactionMgr().getTransactionState(
                    httpStreamParams.getDb().getId(), httpStreamParams.getTxnId());
            if (txnState == null) {
                LOG.warn("Not found http stream related txn, txn id = {}", httpStreamParams.getTxnId());
            } else {
                TxnCoordinator txnCoord = txnState.getCoordinator();
                Backend backend = Env.getCurrentSystemInfo().getBackend(request.getBackendId());
                if (backend != null) {
                    // only modify txnCoord in memory, not write editlog yet.
                    txnCoord.sourceType = TxnSourceType.BE;
                    txnCoord.id = backend.getId();
                    txnCoord.ip = backend.getHost();
                    txnCoord.startTime = backend.getLastStartTime();
                    LOG.info("Change http stream related txn {} to coordinator {}",
                            httpStreamParams.getTxnId(), txnCoord);
                }
            }

            result.setPipelineParams(httpStreamParams.getParams());
            result.getPipelineParams().setDbName(httpStreamParams.getDb().getFullName());
            result.getPipelineParams().setTableName(httpStreamParams.getTable().getName());
            result.getPipelineParams().setTxnConf(new TTxnParams().setTxnId(httpStreamParams.getTxnId()));
            result.getPipelineParams().setImportLabel(httpStreamParams.getLabel());
            result.getPipelineParams()
                    .setIsMowTable(((OlapTable) httpStreamParams.getTable()).getEnableUniqueKeyMergeOnWrite());
            result.setDbId(httpStreamParams.getDb().getId());
            result.setTableId(httpStreamParams.getTable().getId());
            result.setBaseSchemaVersion(((OlapTable) httpStreamParams.getTable()).getBaseSchemaVersion());
            result.setGroupCommitIntervalMs(((OlapTable) httpStreamParams.getTable()).getGroupCommitIntervalMs());
            result.setGroupCommitDataBytes(((OlapTable) httpStreamParams.getTable()).getGroupCommitDataBytes());
            result.setWaitInternalGroupCommitFinish(Config.wait_internal_group_commit_finish);
        } catch (UserException e) {
            LOG.warn("exec sql error", e);
            throw e;
        } catch (Throwable e) {
            LOG.warn("exec sql error catch unknown result.", e);
            throw new UserException("exec sql error catch unknown result." + e);
        }
    }

    @Override
    public TStatus snapshotLoaderReport(TSnapshotLoaderReportRequest request) throws TException {
        if (Env.getCurrentEnv().getBackupHandler().report(request.getTaskType(), request.getJobId(),
                request.getTaskId(), request.getFinishedNum(), request.getTotalNum())) {
            return new TStatus(TStatusCode.OK);
        }
        return new TStatus(TStatusCode.CANCELLED);
    }

    @Override
    public TFrontendReportAliveSessionResult getAliveSessions(TFrontendReportAliveSessionRequest request)
            throws TException {
        TFrontendReportAliveSessionResult result = new TFrontendReportAliveSessionResult();
        result.setStatus(TStatusCode.OK);
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive get alive sessions request: {}", request);
        }

        Env env = Env.getCurrentEnv();
        if (env.isReady()) {
            if (request.getClusterId() != env.getClusterId()) {
                result.setStatus(TStatusCode.INVALID_ARGUMENT);
                result.setMsg("invalid cluster id: " + Env.getCurrentEnv().getClusterId());
            } else if (!request.getToken().equals(env.getToken())) {
                result.setStatus(TStatusCode.INVALID_ARGUMENT);
                result.setMsg("invalid token: " + Env.getCurrentEnv().getToken());
            } else {
                result.setMsg("success");
                result.setSessionIdList(env.getAllAliveSessionIds());
            }
        } else {
            result.setStatus(TStatusCode.ILLEGAL_STATE);
            result.setMsg("not ready");
        }
        return result;
    }

    @Override
    public TFrontendPingFrontendResult ping(TFrontendPingFrontendRequest request) throws TException {
        boolean isReady = Env.getCurrentEnv().isReady();
        TFrontendPingFrontendResult result = new TFrontendPingFrontendResult();
        // The following fields are required in thrift.
        // So must give them a default value to avoid "Required field xx was not present" error.
        result.setStatus(TFrontendPingFrontendStatusCode.OK);
        result.setMsg("");
        result.setQueryPort(0);
        result.setRpcPort(0);
        result.setReplayedJournalId(0);
        result.setVersion(Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH);
        if (isReady) {
            if (request.getClusterId() != Env.getCurrentEnv().getClusterId()) {
                result.setStatus(TFrontendPingFrontendStatusCode.FAILED);
                result.setMsg("invalid cluster id: " + Env.getCurrentEnv().getClusterId());
            }

            if (result.getStatus() == TFrontendPingFrontendStatusCode.OK) {
                // If the version of FE is too old, we need to ensure compatibility.
                if (request.getDeployMode() == null) {
                    LOG.warn("Couldn't find deployMode in heartbeat info, "
                            + "maybe you need upgrade FE master.");
                } else if (!request.getDeployMode().equals(Env.getCurrentEnv().getDeployMode())) {
                    result.setStatus(TFrontendPingFrontendStatusCode.FAILED);
                    result.setMsg("expected deployMode: "
                            + request.getDeployMode()
                            + ", but found deployMode: "
                            + Env.getCurrentEnv().getDeployMode());
                }
            }
            if (result.getStatus() == TFrontendPingFrontendStatusCode.OK) {
                if (!request.getToken().equals(Env.getCurrentEnv().getToken())) {
                    result.setStatus(TFrontendPingFrontendStatusCode.FAILED);
                    result.setMsg("invalid token: " + Env.getCurrentEnv().getToken());
                }
            }

            if (result.status == TFrontendPingFrontendStatusCode.OK) {
                // cluster id and token are valid, return replayed journal id
                long replayedJournalId = Env.getCurrentEnv().getReplayedJournalId();
                result.setMsg("success");
                result.setReplayedJournalId(replayedJournalId);
                result.setQueryPort(Config.query_port);
                result.setRpcPort(Config.rpc_port);
                result.setArrowFlightSqlPort(Config.arrow_flight_sql_port);
                result.setVersion(Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH);
                result.setLastStartupTime(exeEnv.getStartupTime());
                result.setProcessUUID(exeEnv.getProcessUUID());
                if (exeEnv.getDiskInfos() != null) {
                    result.setDiskInfos(FeDiskInfo.toThrifts(exeEnv.getDiskInfos()));
                }
            }
        } else {
            result.setStatus(TFrontendPingFrontendStatusCode.FAILED);
            result.setMsg("not ready");
        }
        return result;
    }

    @Override
    public TFetchSchemaTableDataResult fetchSchemaTableData(TFetchSchemaTableDataRequest request) throws TException {
        try {
            if (!request.isSetSchemaTableName()) {
                return MetadataGenerator.errorResult("Fetch schema table name is not set");
            }
            // tvf queries
            if (request.getSchemaTableName() == TSchemaTableName.METADATA_TABLE) {
                return MetadataGenerator.getMetadataTable(request);
            } else {
                // database information_schema's tables
                return MetadataGenerator.getSchemaTableData(request);
            }
        } catch (Exception e) {
            LOG.warn("Failed to fetchSchemaTableData", e);
            return MetadataGenerator.errorResult(e.getMessage());
        }
    }

    private TNetworkAddress getClientAddr() {
        ThriftServerContext connectionContext = ThriftServerEventProcessor.getConnectionContext();
        // For NonBlockingServer, we can not get client ip.
        if (connectionContext != null) {
            return connectionContext.getClient();
        }
        return null;
    }

    private String getClientAddrAsString() {
        TNetworkAddress addr = getClientAddr();
        return addr == null ? "unknown" : addr.hostname;
    }

    @Override
    public TWaitingTxnStatusResult waitingTxnStatus(TWaitingTxnStatusRequest request) throws TException {
        TWaitingTxnStatusResult result = new TWaitingTxnStatusResult();
        result.setStatus(new TStatus());
        try {
            result = Env.getCurrentGlobalTransactionMgr().getWaitingTxnStatus(request);
            result.status.setStatusCode(TStatusCode.OK);
        } catch (TimeoutException e) {
            result.status.setStatusCode(TStatusCode.INCOMPLETE);
            result.status.addToErrorMsgs(e.getMessage());
        } catch (AnalysisException e) {
            result.status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            result.status.addToErrorMsgs(e.getMessage());
        }
        return result;
    }

    @Override
    public TInitExternalCtlMetaResult initExternalCtlMeta(TInitExternalCtlMetaRequest request) throws TException {
        if (request.isSetCatalogId() && request.isSetDbId()) {
            return initDb(request.catalogId, request.dbId);
        } else if (request.isSetCatalogId()) {
            return initCatalog(request.catalogId);
        } else {
            throw new TException("Catalog name is not set. Init failed.");
        }
    }

    private TInitExternalCtlMetaResult initCatalog(long catalogId) throws TException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        if (!(catalog instanceof ExternalCatalog)) {
            throw new TException("Only support forward ExternalCatalog init operation.");
        }
        TInitExternalCtlMetaResult result = new TInitExternalCtlMetaResult();
        try {
            ((ExternalCatalog) catalog).makeSureInitialized();
            result.setMaxJournalId(Env.getCurrentEnv().getMaxJournalId());
            result.setStatus(MasterCatalogExecutor.STATUS_OK);
        } catch (Throwable t) {
            LOG.warn("init catalog failed. catalog: {}", catalog.getName(), t);
            result.setStatus(Util.getRootCauseStack(t));
        }
        return result;
    }

    private TInitExternalCtlMetaResult initDb(long catalogId, long dbId) throws TException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        if (!(catalog instanceof ExternalCatalog)) {
            throw new TException("Only support forward ExternalCatalog init operation.");
        }
        DatabaseIf db = catalog.getDbNullable(dbId);
        if (db == null) {
            throw new TException("database " + dbId + " is null");
        }
        if (!(db instanceof ExternalDatabase)) {
            throw new TException("Only support forward ExternalDatabase init operation.");
        }

        TInitExternalCtlMetaResult result = new TInitExternalCtlMetaResult();
        try {
            ((ExternalDatabase) db).makeSureInitialized();
            result.setMaxJournalId(Env.getCurrentEnv().getMaxJournalId());
            result.setStatus(MasterCatalogExecutor.STATUS_OK);
        } catch (Throwable t) {
            LOG.warn("init database failed. catalog.database: {}", catalog.getName(), db.getFullName(), t);
            result.setStatus(Util.getRootCauseStack(t));
        }
        return result;
    }

    @Override
    public TMySqlLoadAcquireTokenResult acquireToken() throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive acquire token request from client: {}", clientAddr);
        }
        TMySqlLoadAcquireTokenResult result = new TMySqlLoadAcquireTokenResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            String token = Env.getCurrentEnv().getTokenManager().acquireToken();
            result.setToken(token);
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    @Override
    public boolean checkToken(String token) {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive check token request from client: {}", clientAddr);
        }
        try {
            return Env.getCurrentEnv().getTokenManager().checkAuthToken(token);
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            return false;
        }
    }

    @Override
    public TCheckAuthResult checkAuth(TCheckAuthRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive auth request: {}, backend: {}", request, clientAddr);
        }

        TCheckAuthResult result = new TCheckAuthResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        // check account and password
        final String fullUserName = ClusterNamespace.getNameFromFullName(request.getUser());
        List<UserIdentity> currentUser = Lists.newArrayList();
        try {
            Env.getCurrentEnv().getAuth().checkPlainPassword(fullUserName, request.getUserIp(), request.getPasswd(),
                    currentUser);
        } catch (AuthenticationException e) {
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        Preconditions.checkState(currentUser.size() == 1);
        PrivPredicate predicate = getPrivPredicate(request.getPrivType());
        if (predicate == null) {
            return result;
        }
        // check privilege
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        TPrivilegeCtrl privCtrl = request.getPrivCtrl();
        TPrivilegeHier privHier = privCtrl.getPrivHier();
        if (privHier == TPrivilegeHier.GLOBAL) {
            if (!accessManager.checkGlobalPriv(currentUser.get(0), predicate)) {
                status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
                status.addToErrorMsgs("Global permissions error");
            }
        } else if (privHier == TPrivilegeHier.CATALOG) {
            if (!accessManager.checkCtlPriv(currentUser.get(0), privCtrl.getCtl(), predicate)) {
                status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
                status.addToErrorMsgs("Catalog permissions error");
            }
        } else if (privHier == TPrivilegeHier.DATABASE) {
            String fullDbName = privCtrl.getDb();
            if (!accessManager.checkDbPriv(currentUser.get(0), privCtrl.getCtl(), fullDbName,
                    predicate)) {
                status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
                status.addToErrorMsgs("Database permissions error");
            }
        } else if (privHier == TPrivilegeHier.TABLE) {
            String fullDbName = privCtrl.getDb();
            if (!accessManager.checkTblPriv(currentUser.get(0), privCtrl.getCtl(), fullDbName, privCtrl.getTbl(),
                    predicate)) {
                status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
                status.addToErrorMsgs("Table permissions error");
            }
        } else if (privHier == TPrivilegeHier.COLUMNS) {
            String fullDbName = privCtrl.getDb();

            try {
                accessManager.checkColumnsPriv(currentUser.get(0), InternalCatalog.INTERNAL_CATALOG_NAME, fullDbName,
                        privCtrl.getTbl(), privCtrl.getCols(),
                        predicate);
            } catch (UserException e) {
                status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
                status.addToErrorMsgs("Columns permissions error:" + e.getMessage());
            }
        } else if (privHier == TPrivilegeHier.RESOURSE) {
            if (!accessManager.checkResourcePriv(currentUser.get(0), privCtrl.getRes(), predicate)) {
                status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
                status.addToErrorMsgs("Resourse permissions error");
            }
        } else {
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs("Privilege control error");
        }
        return result;
    }

    private PrivPredicate getPrivPredicate(TPrivilegeType privType) {
        if (privType == null) {
            return null;
        }
        switch (privType) {
            case SHOW:
                return PrivPredicate.SHOW;
            case SHOW_RESOURCES:
                return PrivPredicate.SHOW_RESOURCES;
            case GRANT:
                return PrivPredicate.GRANT;
            case ADMIN:
                return PrivPredicate.ADMIN;
            case LOAD:
                return PrivPredicate.LOAD;
            case ALTER:
                return PrivPredicate.ALTER;
            case USAGE:
                return PrivPredicate.USAGE;
            case CREATE:
                return PrivPredicate.CREATE;
            case ALL:
                return PrivPredicate.ALL;
            case OPERATOR:
                return PrivPredicate.OPERATOR;
            case DROP:
                return PrivPredicate.DROP;
            default:
                return null;
        }
    }

    @Override
    public TQueryStatsResult getQueryStats(TGetQueryStatsRequest request) throws TException {
        TQueryStatsResult result = new TQueryStatsResult();
        result.setStatus(new TStatus(TStatusCode.OK));
        if (!request.isSetType()) {
            TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs("type is not set");
            result.setStatus(status);
            return result;
        }
        try {
            switch (request.getType()) {
                case CATALOG: {
                    if (!request.isSetCatalog()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("catalog is not set");
                        result.setStatus(status);
                    } else {
                        result.setSimpleResult(Env.getCurrentEnv().getQueryStats().getCatalogStats(request.catalog));
                    }
                    break;
                }
                case DATABASE: {
                    if (!request.isSetCatalog() || !request.isSetDb()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("catalog or db is not set");
                        result.setStatus(status);
                        return result;
                    } else {
                        result.setSimpleResult(
                                Env.getCurrentEnv().getQueryStats().getDbStats(request.catalog, request.db));
                    }
                    break;
                }
                case TABLE: {
                    if (!request.isSetCatalog() || !request.isSetDb() || !request.isSetTbl()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("catalog or db or table is not set");
                        result.setStatus(status);
                        return result;
                    } else {
                        Env.getCurrentEnv().getQueryStats().getTblStats(request.catalog, request.db, request.tbl)
                                .forEach((col, stat) -> {
                                    TTableQueryStats colunmStat = new TTableQueryStats();
                                    colunmStat.setField(col);
                                    colunmStat.setQueryStats(stat.first);
                                    colunmStat.setFilterStats(stat.second);
                                    result.addToTableStats(colunmStat);
                                });
                    }
                    break;
                }
                case TABLE_ALL: {
                    if (!request.isSetCatalog() || !request.isSetDb() || !request.isSetTbl()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("catalog or db or table is not set");
                        result.setStatus(status);
                    } else {
                        result.setSimpleResult(Env.getCurrentEnv().getQueryStats()
                                .getTblAllStats(request.catalog, request.db, request.tbl));
                    }
                    break;
                }
                case TABLE_ALL_VERBOSE: {
                    if (!request.isSetCatalog() || !request.isSetDb() || !request.isSetTbl()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("catalog or db or table is not set");
                        result.setStatus(status);
                    } else {
                        Env.getCurrentEnv().getQueryStats()
                                .getTblAllVerboseStats(request.catalog, request.db, request.tbl)
                                .forEach((indexName, indexStats) -> {
                                    TTableIndexQueryStats indexStat = new TTableIndexQueryStats();
                                    indexStat.setIndexName(indexName);
                                    indexStats.forEach((col, stat) -> {
                                        TTableQueryStats colunmStat = new TTableQueryStats();
                                        colunmStat.setField(col);
                                        colunmStat.setQueryStats(stat.first);
                                        colunmStat.setFilterStats(stat.second);
                                        indexStat.addToTableStats(colunmStat);
                                    });
                                    result.addToTableVerbosStats(indexStat);
                                });
                    }
                    break;
                }
                case TABLET: {
                    if (!request.isSetReplicaId()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("Replica Id is not set");
                        result.setStatus(status);
                    } else {
                        Map<Long, Long> tabletStats = new HashMap<>();
                        tabletStats.put(request.getReplicaId(),
                                Env.getCurrentEnv().getQueryStats().getStats(request.getReplicaId()));
                        result.setTabletStats(tabletStats);
                    }
                    break;
                }
                case TABLETS: {
                    if (!request.isSetReplicaIds()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("Replica Ids is not set");
                        result.setStatus(status);
                    } else {
                        Map<Long, Long> tabletStats = new HashMap<>();
                        QueryStats qs = Env.getCurrentEnv().getQueryStats();
                        for (long replicaId : request.getReplicaIds()) {
                            tabletStats.put(replicaId, qs.getStats(replicaId));
                        }
                        result.setTabletStats(tabletStats);
                    }
                    break;
                }
                default: {
                    TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                    status.addToErrorMsgs("unknown type: " + request.getType());
                    result.setStatus(status);
                    break;
                }
            }
        } catch (UserException e) {
            TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
            result.setStatus(status);
        }
        return result;
    }

    public TGetTabletReplicaInfosResult getTabletReplicaInfos(TGetTabletReplicaInfosRequest request) {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive get replicas request: {}, backend: {}", request, clientAddr);
        }
        TGetTabletReplicaInfosResult result = new TGetTabletReplicaInfosResult();
        List<Long> tabletIds = request.getTabletIds();
        Map<Long, List<TReplicaInfo>> tabletReplicaInfos = Maps.newHashMap();
        for (Long tabletId : tabletIds) {
            if (DebugPointUtil.isEnable("getTabletReplicaInfos.returnEmpty")) {
                LOG.info("enable getTabletReplicaInfos.returnEmpty");
                continue;
            }
            List<TReplicaInfo> replicaInfos = Lists.newArrayList();
            List<Replica> replicas = Env.getCurrentEnv().getCurrentInvertedIndex()
                    .getReplicasByTabletId(tabletId);
            for (Replica replica : replicas) {
                if (!replica.isNormal()) {
                    LOG.warn("replica {} not normal", replica.getId());
                    continue;
                }
                Backend backend = Env.getCurrentSystemInfo().getBackend(replica.getBackendIdWithoutException());
                if (backend != null) {
                    TReplicaInfo replicaInfo = new TReplicaInfo();
                    replicaInfo.setHost(backend.getHost());
                    replicaInfo.setBePort(backend.getBePort());
                    replicaInfo.setHttpPort(backend.getHttpPort());
                    replicaInfo.setBrpcPort(backend.getBrpcPort());
                    replicaInfo.setIsAlive(backend.isAlive());
                    replicaInfo.setBackendId(backend.getId());
                    replicaInfo.setReplicaId(replica.getId());
                    replicaInfos.add(replicaInfo);
                }
            }
            tabletReplicaInfos.put(tabletId, replicaInfos);
        }
        result.setTabletReplicaInfos(tabletReplicaInfos);
        result.setToken(Env.getCurrentEnv().getToken());
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    @Override
    public TAutoIncrementRangeResult getAutoIncrementRange(TAutoIncrementRangeRequest request) {
        String clientAddr = getClientAddrAsString();
        LOG.info("[auto-inc] receive getAutoIncrementRange request: {}, backend: {}", request, clientAddr);

        TAutoIncrementRangeResult result = new TAutoIncrementRangeResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("[auto-inc] failed to getAutoIncrementRange:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, getClientAddrAsString());
            return result;
        }

        try {
            Env env = Env.getCurrentEnv();
            Database db = env.getInternalCatalog().getDbOrMetaException(request.getDbId());
            OlapTable olapTable = (OlapTable) db.getTableOrMetaException(request.getTableId(), TableType.OLAP);
            AutoIncrementGenerator autoIncrementGenerator = null;
            autoIncrementGenerator = olapTable.getAutoIncrementGenerator();
            long columnId = request.getColumnId();
            long length = request.getLength();
            Pair<Long, Long> range = autoIncrementGenerator.getAutoIncrementRange(columnId, length, -1);
            result.setStart(range.first);
            result.setLength(range.second);
        } catch (UserException e) {
            LOG.warn("[auto-inc] failed to get auto-increment range of db_id={}, table_id={}, column_id={}, errmsg={}",
                    request.getDbId(), request.getTableId(), request.getColumnId(), e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("[auto-inc] catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(e.getClass().getSimpleName() + ": " + Strings.nullToEmpty(e.getMessage()));
        }
        return result;
    }

    public TGetBinlogResult getBinlog(TGetBinlogRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive get binlog request: {}", request);
        }

        TGetBinlogResult result = new TGetBinlogResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        boolean syncJournal = false;
        if (!Env.getCurrentEnv().isMaster()) {
            if (!request.isAllowFollowerRead()) {
                status.setStatusCode(TStatusCode.NOT_MASTER);
                status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
                result.setMasterAddress(getMasterAddress());
                LOG.error("failed to get binlog: {}", NOT_MASTER_ERR_MSG);
                return result;
            }
            syncJournal = true;
        }

        try {
            /// Check all required arg: user, passwd, db, prev_commit_seq
            if (!request.isSetUser()) {
                throw new UserException("user is not set");
            }
            if (!request.isSetPasswd()) {
                throw new UserException("passwd is not set");
            }
            if (!request.isSetDb()) {
                throw new UserException("db is not set");
            }
            if (!request.isSetPrevCommitSeq()) {
                throw new UserException("prev_commit_seq is not set");
            }

            if (syncJournal) {
                ConnectContext ctx = new ConnectContext(null);
                ctx.setDatabase(request.getDb());
                ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(request.getUser(), "%"));
                ctx.setEnv(Env.getCurrentEnv());
                MasterOpExecutor executor = new MasterOpExecutor(ctx);
                executor.syncJournal();
            }

            result = getBinlogImpl(request, clientAddr);
        } catch (UserException e) {
            LOG.warn("failed to get binlog: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    private TGetBinlogResult getBinlogImpl(TGetBinlogRequest request, String clientIp) throws UserException {
        // step 1: check auth
        if (Strings.isNullOrEmpty(request.getToken())) {
            checkSingleTablePasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTable(),
                    request.getUserIp(), PrivPredicate.SELECT);
        }

        // step 3: check database
        Env env = Env.getCurrentEnv();
        String fullDbName = request.getDb();
        Database db = env.getInternalCatalog().getDbNullable(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        // step 4: fetch all tableIds
        // lookup tables && convert into tableIdList
        long tableId = -1;
        if (request.isSetTableId()) {
            tableId = request.getTableId();
        } else if (request.isSetTable()) {
            String tableName = request.getTable();
            Table table = db.getTableOrMetaException(tableName, TableType.OLAP);
            if (table == null) {
                throw new UserException("unknown table, table=" + tableName);
            }
            tableId = table.getId();
        }

        // step 6: get binlog
        long dbId = db.getId();
        TGetBinlogResult result = new TGetBinlogResult();
        result.setStatus(new TStatus(TStatusCode.OK));
        long prevCommitSeq = request.getPrevCommitSeq();
        long numAcquired = request.getNumAcquired();
        Pair<TStatus, List<TBinlog>> statusBinlogPair = env.getBinlogManager()
                .getBinlog(dbId, tableId, prevCommitSeq, numAcquired);
        TStatus status = statusBinlogPair.first;
        if (status != null && status.getStatusCode() != TStatusCode.OK) {
            result.setStatus(status);
            // TOO_OLD return first exist binlog
            if (status.getStatusCode() != TStatusCode.BINLOG_TOO_OLD_COMMIT_SEQ) {
                return result;
            }
        }
        List<TBinlog> binlogs = statusBinlogPair.second;
        if (binlogs != null) {
            result.setBinlogs(binlogs);
        }
        return result;
    }

    // getSnapshot
    public TGetSnapshotResult getSnapshot(TGetSnapshotRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.trace("receive get snapshot info request: {}", request);

        TGetSnapshotResult result = new TGetSnapshotResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get getSnapshot: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        try {
            result = getSnapshotImpl(request, clientAddr);
        } catch (UserException e) {
            LOG.warn("failed to get snapshot info: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    // getSnapshotImpl
    private TGetSnapshotResult getSnapshotImpl(TGetSnapshotRequest request, String clientIp)
            throws UserException, IOException {
        // Step 1: Check all required arg: user, passwd, db, label_name, snapshot_name,
        // snapshot_type
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }
        if (!request.isSetLabelName()) {
            throw new UserException("label_name is not set");
        }
        if (!request.isSetSnapshotName()) {
            throw new UserException("snapshot_name is not set");
        }
        if (!request.isSetSnapshotType()) {
            throw new UserException("snapshot_type is not set");
        } else if (request.getSnapshotType() != TSnapshotType.LOCAL) {
            throw new UserException("snapshot_type is not LOCAL");
        }

        LOG.info("get snapshot info, user: {}, db: {}, label_name: {}, snapshot_name: {}, snapshot_type: {}",
                request.getUser(), request.getDb(), request.getLabelName(), request.getSnapshotName(),
                request.getSnapshotType());
        if (Strings.isNullOrEmpty(request.getToken())) {
            checkSingleTablePasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTable(), clientIp, PrivPredicate.SELECT);
        }

        // Step 3: get snapshot
        String label = request.getLabelName();
        TGetSnapshotResult result = new TGetSnapshotResult();
        result.setStatus(new TStatus(TStatusCode.OK));
        Snapshot snapshot = Env.getCurrentEnv().getBackupHandler().getSnapshot(label);
        if (snapshot == null) {
            result.getStatus().setStatusCode(TStatusCode.SNAPSHOT_NOT_EXIST);
            result.getStatus().addToErrorMsgs(String.format("snapshot %s not exist", label));
        } else if (snapshot.isExpired()) {
            result.getStatus().setStatusCode(TStatusCode.SNAPSHOT_EXPIRED);
            result.getStatus().addToErrorMsgs(String.format("snapshot %s is expired", label));
        } else {
            byte[] meta = snapshot.getMeta();
            byte[] jobInfo = snapshot.getJobInfo();
            long expiredAt = snapshot.getExpiredAt();
            long commitSeq = snapshot.getCommitSeq();

            LOG.info("get snapshot info, snapshot: {}, meta size: {}, job info size: {}, "
                    + "expired at: {}, commit seq: {}", label, meta.length, jobInfo.length, expiredAt, commitSeq);
            if (request.isEnableCompress()) {
                meta = GZIPUtils.compress(meta);
                jobInfo = GZIPUtils.compress(jobInfo);
                result.setCompressed(true);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("get snapshot info with compress, snapshot: {}, compressed meta "
                            + "size {}, compressed job info size {}", label, meta.length, jobInfo.length);
                }
            }
            result.setMeta(meta);
            result.setJobInfo(jobInfo);
            result.setExpiredAt(expiredAt);
            result.setCommitSeq(commitSeq);
        }

        return result;
    }

    // Restore Snapshot
    public TRestoreSnapshotResult restoreSnapshot(TRestoreSnapshotRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.trace("receive restore snapshot info request: {}", request);

        TRestoreSnapshotResult result = new TRestoreSnapshotResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get restoreSnapshot: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        try {
            result = restoreSnapshotImpl(request, clientAddr);
        } catch (UserException e) {
            LOG.warn("failed to get snapshot info: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    // restoreSnapshotImpl
    private TRestoreSnapshotResult restoreSnapshotImpl(TRestoreSnapshotRequest request, String clientIp)
            throws UserException {
        // Step 1: Check all required arg: user, passwd, db, label_name, repo_name,
        // meta, info
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }
        if (!request.isSetLabelName()) {
            throw new UserException("label_name is not set");
        }
        if (!request.isSetRepoName()) {
            throw new UserException("repo_name is not set");
        }
        if (!request.isSetMeta()) {
            throw new UserException("meta is not set");
        }
        if (!request.isSetJobInfo()) {
            throw new UserException("job_info is not set");
        }

        // Step 2: check auth
        if (Strings.isNullOrEmpty(request.getToken())) {
            checkDbPasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(), clientIp,
                    PrivPredicate.LOAD);
        }

        // Step 3: get snapshot
        TRestoreSnapshotResult result = new TRestoreSnapshotResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        LabelName label = new LabelName(request.getDb(), request.getLabelName());
        String repoName = request.getRepoName();
        Map<String, String> properties = request.getProperties();

        // Restore requires that all properties are known, so the old version of FE will not be able
        // to recognize the properties of the new version. Therefore, request parameters are used here
        // instead of directly putting them in properties to avoid compatibility issues of cross-version
        // synchronization.
        if (request.isCleanPartitions()) {
            properties.put(RestoreCommand.PROP_CLEAN_PARTITIONS, "true");
        }
        if (request.isCleanTables()) {
            properties.put(RestoreCommand.PROP_CLEAN_TABLES, "true");
        }
        if (request.isAtomicRestore()) {
            properties.put(RestoreCommand.PROP_ATOMIC_RESTORE, "true");
        }
        if (request.isForceReplace()) {
            properties.put(RestoreCommand.PROP_FORCE_REPLACE, "true");
        }

        AbstractBackupTableRefClause restoreTableRefClause = null;
        if (request.isSetTableRefs()) {
            List<TableRef> tableRefs = new ArrayList<>();
            for (TTableRef tTableRef : request.getTableRefs()) {
                tableRefs.add(new TableRef(new TableName(tTableRef.getTable()), tTableRef.getAliasName()));
            }

            if (tableRefs.size() > 0) {
                boolean isExclude = false;
                restoreTableRefClause = new AbstractBackupTableRefClause(isExclude, tableRefs);
            }
        }

        byte[] meta = request.getMeta();
        byte[] jobInfo = request.getJobInfo();
        if (Config.enable_restore_snapshot_rpc_compression && request.isCompressed()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("decompress meta and job info, compressed meta size {}, compressed job info size {}",
                        meta.length, jobInfo.length);
            }
            try {
                meta = GZIPUtils.decompress(meta);
                jobInfo = GZIPUtils.decompress(jobInfo);
            } catch (Exception e) {
                LOG.warn("decompress meta and job info failed", e);
                throw new UserException("decompress meta and job info failed", e);
            }
        } else if (GZIPUtils.isGZIPCompressed(jobInfo) || GZIPUtils.isGZIPCompressed(meta)) {
            throw new UserException("The request is compressed, but the config "
                    + "`enable_restore_snapshot_rpc_compressed` is not enabled.");
        }

        //instantiate RestoreCommand
        LabelNameInfo labelNameInfo = new LabelNameInfo(label.getDbName(), label.getLabelName());
        List<TableRefInfo> tableRefInfos = new ArrayList<>();
        List<TableRef> tableRefList = restoreTableRefClause == null
                ? new ArrayList<>() : restoreTableRefClause.getTableRefList();
        for (TableRef tableRef : tableRefList) {
            TableName tableName = tableRef.getName();
            String[] aliases = tableRef.getAliases();
            PartitionNames partitionNames = tableRef.getPartitionNames();
            List<Long> sampleTabletIds = tableRef.getSampleTabletIds();
            TableSample tableSample = tableRef.getTableSample();
            ArrayList<String> commonHints = tableRef.getCommonHints();
            TableSnapshot tableSnapshot = tableRef.getTableSnapshot();
            TableScanParams tableScanParams = tableRef.getScanParams();

            TableNameInfo tableNameInfo = null;
            if (tableName != null) {
                tableNameInfo = new TableNameInfo(tableName.getCtl(), tableName.getDb(), tableName.getTbl());
            }

            String tableAlias = aliases.length >= 1 ? aliases[0] : null;

            PartitionNamesInfo partitionNamesInfo = null;
            if (partitionNames != null) {
                partitionNamesInfo = new PartitionNamesInfo(partitionNames.isTemp(),
                        partitionNames.isStar(),
                        partitionNames.getPartitionNames(),
                        0);
            }

            List<Long> tabletIdList = new ArrayList<>();
            if (sampleTabletIds != null) {
                tabletIdList.addAll(sampleTabletIds);
            }

            ArrayList<String> relationHints = new ArrayList<>();
            if (commonHints != null) {
                relationHints.addAll(commonHints);
            }

            org.apache.doris.nereids.trees.TableSample newTableSample =
                    new org.apache.doris.nereids.trees.TableSample(tableSample.getSampleValue(),
                            tableSample.isPercent(),
                            tableSample.getSeek());

            TableRefInfo tableRefInfo = new TableRefInfo(tableNameInfo,
                    tableScanParams,
                    tableSnapshot,
                    partitionNamesInfo,
                    tabletIdList,
                    tableAlias,
                    newTableSample,
                    relationHints);
            tableRefInfos.add(tableRefInfo);
        }

        RestoreCommand restoreCommand = new RestoreCommand(labelNameInfo, repoName, tableRefInfos, properties, false);
        restoreCommand.setMeta(meta);
        restoreCommand.setJobInfo(jobInfo);
        restoreCommand.setIsBeingSynced();
        LOG.debug("restore snapshot info, restoreCommand: {}", restoreCommand);
        try {
            ConnectContext ctx = new ConnectContext();
            String fullUserName = ClusterNamespace.getNameFromFullName(request.getUser());
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(fullUserName, "%"));
            ctx.setThreadLocalInfo();
            restoreCommand.validate(ctx);
            Env.getCurrentEnv().getBackupHandler().process(restoreCommand);
        } catch (UserException e) {
            LOG.warn("failed to restore: {}, command: {}", e.getMessage(), restoreCommand, e);
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage() + ", command: " + restoreCommand);
        } catch (Throwable e) {
            LOG.warn("catch unknown result. command: {}", restoreCommand, e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()) + ", command: " + restoreCommand);
        } finally {
            ConnectContext.remove();
        }

        return result;
    }

    @Override
    public TPlsqlStoredProcedureResult addPlsqlStoredProcedure(TAddPlsqlStoredProcedureRequest request) {
        TPlsqlStoredProcedureResult result = new TPlsqlStoredProcedureResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to addPlsqlStoredProcedure:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, getClientAddrAsString());
            return result;
        }

        if (!request.isSetPlsqlStoredProcedure()) {
            status.setStatusCode(TStatusCode.INVALID_ARGUMENT);
            status.addToErrorMsgs("missing stored procedure.");
            return result;
        }
        try {
            Env.getCurrentEnv().getPlsqlManager()
                    .addPlsqlStoredProcedure(PlsqlStoredProcedure.fromThrift(request.getPlsqlStoredProcedure()),
                            request.isSetIsForce() && request.isIsForce());
        } catch (RuntimeException e) {
            status.setStatusCode(TStatusCode.ALREADY_EXIST);
            status.addToErrorMsgs(e.getMessage());
            return result;
        }
        return result;
    }

    @Override
    public TPlsqlStoredProcedureResult dropPlsqlStoredProcedure(TDropPlsqlStoredProcedureRequest request) {
        TPlsqlStoredProcedureResult result = new TPlsqlStoredProcedureResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to dropPlsqlStoredProcedure:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, getClientAddrAsString());
            return result;
        }

        if (!request.isSetPlsqlProcedureKey()) {
            status.setStatusCode(TStatusCode.INVALID_ARGUMENT);
            status.addToErrorMsgs("missing stored key.");
            return result;
        }

        Env.getCurrentEnv().getPlsqlManager().dropPlsqlStoredProcedure(PlsqlProcedureKey.fromThrift(
                request.getPlsqlProcedureKey()));
        return result;
    }

    @Override
    public TPlsqlPackageResult addPlsqlPackage(TAddPlsqlPackageRequest request) throws TException {
        TPlsqlPackageResult result = new TPlsqlPackageResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to addPlsqlPackage:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, getClientAddrAsString());
            return result;
        }

        if (!request.isSetPlsqlPackage()) {
            status.setStatusCode(TStatusCode.INVALID_ARGUMENT);
            status.addToErrorMsgs("missing plsql package.");
            return result;
        }

        try {
            Env.getCurrentEnv().getPlsqlManager().addPackage(PlsqlPackage.fromThrift(request.getPlsqlPackage()),
                    request.isSetIsForce() && request.isIsForce());
        } catch (RuntimeException e) {
            status.setStatusCode(TStatusCode.ALREADY_EXIST);
            status.addToErrorMsgs(e.getMessage());
            return result;
        }
        return result;
    }

    @Override
    public TPlsqlPackageResult dropPlsqlPackage(TDropPlsqlPackageRequest request) throws TException {
        TPlsqlPackageResult result = new TPlsqlPackageResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to dropPlsqlPackage:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, getClientAddrAsString());
            return result;
        }

        if (!request.isSetPlsqlProcedureKey()) {
            status.setStatusCode(TStatusCode.INVALID_ARGUMENT);
            status.addToErrorMsgs("missing stored key.");
            return result;
        }

        Env.getCurrentEnv().getPlsqlManager().dropPackage(PlsqlProcedureKey.fromThrift(request.getPlsqlProcedureKey()));
        return result;
    }

    public TGetMasterTokenResult getMasterToken(TGetMasterTokenRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive get master token request: {}", request);
        }

        TGetMasterTokenResult result = new TGetMasterTokenResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get getMasterToken: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        try {
            checkPassword(request.getUser(), request.getPassword(), clientAddr);
            result.setToken(Env.getCurrentEnv().getToken());
        } catch (AuthenticationException e) {
            LOG.warn("failed to get master token: {}", e.getMessage());
            status.setStatusCode(TStatusCode.NOT_AUTHORIZED);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
        }

        return result;
    }

    // getBinlogLag
    public TGetBinlogLagResult getBinlogLag(TGetBinlogRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive get binlog lag request: {}", request);
        }

        TGetBinlogLagResult result = new TGetBinlogLagResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get binlog lag: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        try {
            result = getBinlogLagImpl(request, clientAddr);
        } catch (UserException e) {
            LOG.warn("failed to get binlog lag: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    private TGetBinlogLagResult getBinlogLagImpl(TGetBinlogRequest request, String clientIp) throws UserException {
        /// Check all required arg: user, passwd, db, prev_commit_seq
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }
        if (!request.isSetPrevCommitSeq()) {
            throw new UserException("prev_commit_seq is not set");
        }

        // step 1: check auth
        if (Strings.isNullOrEmpty(request.getToken())) {
            checkSingleTablePasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTable(),
                    request.getUserIp(), PrivPredicate.SELECT);
        }

        // step 3: check database
        Env env = Env.getCurrentEnv();
        String fullDbName = request.getDb();
        Database db = env.getInternalCatalog().getDbNullable(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        // step 4: fetch all tableIds
        // lookup tables && convert into tableIdList
        long tableId = -1;
        if (request.isSetTableId()) {
            tableId = request.getTableId();
        } else if (request.isSetTable()) {
            String tableName = request.getTable();
            Table table = db.getTableOrMetaException(tableName, TableType.OLAP);
            if (table == null) {
                throw new UserException("unknown table, table=" + tableName);
            }
            tableId = table.getId();
        }

        // step 6: get binlog
        long dbId = db.getId();
        TGetBinlogLagResult result = new TGetBinlogLagResult();
        result.setStatus(new TStatus(TStatusCode.OK));
        long prevCommitSeq = request.getPrevCommitSeq();

        Pair<TStatus, BinlogLagInfo> binlogLagInfo = env.getBinlogManager().getBinlogLag(dbId, tableId, prevCommitSeq);
        TStatus status = binlogLagInfo.first;
        if (status != null && status.getStatusCode() != TStatusCode.OK) {
            result.setStatus(status);
        }
        BinlogLagInfo lagInfo = binlogLagInfo.second;
        if (lagInfo != null) {
            result.setLag(lagInfo.getLag());
            result.setFirstCommitSeq(lagInfo.getFirstCommitSeq());
            result.setLastCommitSeq(lagInfo.getLastCommitSeq());
            result.setFirstBinlogTimestamp(lagInfo.getFirstCommitTs());
            result.setLastBinlogTimestamp(lagInfo.getLastCommitTs());
            result.setNextCommitSeq(lagInfo.getNextCommitSeq());
            result.setNextBinlogTimestamp(lagInfo.getNextCommitTs());
        }
        return result;
    }

    public TLockBinlogResult lockBinlog(TLockBinlogRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive lock binlog request: {}", request);
        }

        TLockBinlogResult result = new TLockBinlogResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to lock binlog: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        try {
            result = lockBinlogImpl(request, clientAddr);
        } catch (UserException e) {
            LOG.warn("failed to lock binlog: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    private TLockBinlogResult lockBinlogImpl(TLockBinlogRequest request, String clientIp) throws UserException {
        /// Check all required arg: user, passwd, db, prev_commit_seq
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }
        if (!request.isSetJobUniqueId()) {
            throw new UserException("job_unique_id is not set");
        }

        // step 1: check auth
        if (Strings.isNullOrEmpty(request.getToken())) {
            checkSingleTablePasswordAndPrivs(request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTable(), clientIp, PrivPredicate.SELECT);
        }

        // step 3: check database
        Env env = Env.getCurrentEnv();
        String fullDbName = request.getDb();
        Database db = env.getInternalCatalog().getDbNullable(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        // step 4: fetch all tableIds
        // lookup tables && convert into tableIdList
        long tableId = -1;
        if (request.isSetTableId()) {
            tableId = request.getTableId();
        } else if (request.isSetTable()) {
            String tableName = request.getTable();
            Table table = db.getTableOrMetaException(tableName, TableType.OLAP);
            if (table == null) {
                throw new UserException("unknown table, table=" + tableName);
            }
            tableId = table.getId();
        }

        // step 6: lock binlog
        long dbId = db.getId();
        String jobUniqueId = request.getJobUniqueId();
        long lockCommitSeq = -1L;
        if (request.isSetLockCommitSeq()) {
            lockCommitSeq = request.getLockCommitSeq();
        }
        Pair<TStatus, Long> statusSeqPair = env.getBinlogManager().lockBinlog(
                dbId, tableId, jobUniqueId, lockCommitSeq);
        TLockBinlogResult result = new TLockBinlogResult();
        result.setStatus(statusSeqPair.first);
        result.setLockedCommitSeq(statusSeqPair.second);
        return result;
    }

    @Override
    public TStatus updateStatsCache(TUpdateFollowerStatsCacheRequest request) throws TException {
        StatisticsCacheKey k = GsonUtils.GSON.fromJson(request.key, StatisticsCacheKey.class);
        ColStatsData data = GsonUtils.GSON.fromJson(request.colStatsData, ColStatsData.class);
        ColumnStatistic c = data.toColumnStatistic();
        if (c == ColumnStatistic.UNKNOWN) {
            Env.getCurrentEnv().getStatisticsCache().invalidateColumnStatsCache(k.catalogId, k.dbId, k.tableId,
                    k.idxId, k.colName);
        } else {
            Env.getCurrentEnv().getStatisticsCache().updateColStatsCache(
                    k.catalogId, k.dbId, k.tableId, k.idxId, k.colName, c);
        }
        // Return Ok anyway
        return new TStatus(TStatusCode.OK);
    }

    @Override
    public TStatus updatePlanStatsCache(TUpdatePlanStatsCacheRequest request) throws TException {
        PlanNodeAndHash key = GsonUtils.GSON.fromJson(request.key, PlanNodeAndHash.class);
        RecentRunsPlanStatistics data = GsonUtils.GSON.fromJson(request.planStatsData, RecentRunsPlanStatistics.class);
        Env.getCurrentEnv().getHboPlanStatisticsManager().getHboPlanStatisticsProvider()
                .updatePlanStats(key, data);
        return new TStatus(TStatusCode.OK);
    }

    @Override
    public TStatus invalidateStatsCache(TInvalidateFollowerStatsCacheRequest request) throws TException {
        InvalidateStatsTarget target = GsonUtils.GSON.fromJson(request.key, InvalidateStatsTarget.class);
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        TableStatsMeta tableStats = analysisManager.findTableStatsStatus(target.tableId);
        PartitionNames partitionNames = null;
        if (target.partitions != null) {
            partitionNames = new PartitionNames(false, new ArrayList<>(target.partitions));
        }
        if (target.isTruncate) {
            analysisManager.submitAsyncDropStatsTask(target.catalogId, target.dbId,
                    target.tableId, partitionNames, false);
        } else {
            analysisManager.invalidateLocalStats(target.catalogId, target.dbId, target.tableId,
                    target.columns, tableStats, partitionNames);
        }
        return new TStatus(TStatusCode.OK);
    }

    @Override
    public TCreatePartitionResult createPartition(TCreatePartitionRequest request) throws TException {
        LOG.info("Receive create partition request: {}", request);
        long dbId = request.getDbId();
        long tableId = request.getTableId();
        TCreatePartitionResult result = new TCreatePartitionResult();
        TStatus errorStatus = new TStatus(TStatusCode.RUNTIME_ERROR);
        if (!Env.getCurrentEnv().isMaster()) {
            errorStatus.setStatusCode(TStatusCode.NOT_MASTER);
            errorStatus.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.warn("failed to createPartition: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            errorStatus.setErrorMsgs(Lists.newArrayList(String.format("dbId=%d is not exists", dbId)));
            result.setStatus(errorStatus);
            LOG.warn("send create partition error status: {}", result);
            return result;
        }

        Table table = db.getTable(tableId).get();
        if (table == null) {
            errorStatus.setErrorMsgs(
                    (Lists.newArrayList(String.format("dbId=%d tableId=%d is not exists", dbId, tableId))));
            result.setStatus(errorStatus);
            LOG.warn("send create partition error status: {}", result);
            return result;
        }

        if (!(table instanceof OlapTable)) {
            errorStatus.setErrorMsgs(
                    Lists.newArrayList(String.format("dbId=%d tableId=%d is not olap table", dbId, tableId)));
            result.setStatus(errorStatus);
            LOG.warn("send create partition error status: {}", result);
            return result;
        }

        if (request.partitionValues == null) {
            errorStatus.setErrorMsgs(Lists.newArrayList("partitionValues should not null."));
            result.setStatus(errorStatus);
            LOG.warn("send create partition error status: {}", result);
            return result;
        }

        OlapTable olapTable = (OlapTable) table;
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        ArrayList<List<TNullableStringLiteral>> partitionValues = new ArrayList<>();
        for (int i = 0; i < request.partitionValues.size(); i++) {
            if (partitionInfo.getType() == PartitionType.RANGE && request.partitionValues.get(i).size() != 1) {
                errorStatus.setErrorMsgs(
                        Lists.newArrayList(
                                "Only support single partition of RANGE, partitionValues size should equal 1."));
                result.setStatus(errorStatus);
                LOG.warn("send create partition error status: {}", result);
                return result;
            }
            partitionValues.add(request.partitionValues.get(i));
        }
        Map<String, AddPartitionClause> addPartitionClauseMap;
        try {
            addPartitionClauseMap = PartitionExprUtil.getAddPartitionClauseFromPartitionValues(olapTable,
                    partitionValues, partitionInfo);
        } catch (AnalysisException ex) {
            errorStatus.setErrorMsgs(Lists.newArrayList(ex.getMessage()));
            result.setStatus(errorStatus);
            LOG.warn("send create partition error status: {}", result);
            return result;
        }

        for (AddPartitionClause addPartitionClause : addPartitionClauseMap.values()) {
            try {
                // here maybe check and limit created partitions num
                Env.getCurrentEnv().addPartition(db, olapTable.getName(), addPartitionClause, false, 0, true);
            } catch (DdlException e) {
                LOG.warn(e);
                errorStatus.setErrorMsgs(
                        Lists.newArrayList(String.format("create partition failed. error:%s", e.getMessage())));
                result.setStatus(errorStatus);
                LOG.warn("send create partition error status: {}", result);
                return result;
            }
        }

        // check partition's number limit. because partitions in addPartitionClauseMap may be duplicated with existing
        // partitions, which would lead to false positive. so we should check the partition number AFTER adding new
        // partitions using its ACTUAL NUMBER, rather than the sum of existing and requested partitions.
        if (olapTable.getPartitionNum() > Config.max_auto_partition_num) {
            String errorMessage = String.format(
                    "partition numbers %d exceeded limit of variable max_auto_partition_num %d",
                    olapTable.getPartitionNum(), Config.max_auto_partition_num);
            LOG.warn(errorMessage);
            errorStatus.setErrorMsgs(Lists.newArrayList(errorMessage));
            result.setStatus(errorStatus);
            LOG.warn("send create partition error status: {}", result);
            return result;
        }

        // build partition & tablets
        List<TOlapTablePartition> partitions = Lists.newArrayList();
        List<TTabletLocation> tablets = Lists.newArrayList();
        List<TTabletLocation> slaveTablets = new ArrayList<>();
        for (String partitionName : addPartitionClauseMap.keySet()) {
            Partition partition = table.getPartition(partitionName);
            TOlapTablePartition tPartition = new TOlapTablePartition();
            tPartition.setId(partition.getId());
            int partColNum = partitionInfo.getPartitionColumns().size();
            try {
                OlapTableSink.setPartitionKeys(tPartition, partitionInfo.getItem(partition.getId()), partColNum);
            } catch (UserException ex) {
                errorStatus.setErrorMsgs(Lists.newArrayList(ex.getMessage()));
                result.setStatus(errorStatus);
                LOG.warn("send create partition error status: {}", result);
                return result;
            }
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                        index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
                tPartition.setNumBuckets(index.getTablets().size());
            }
            tPartition.setIsMutable(olapTable.getPartitionInfo().getIsMutable(partition.getId()));
            partitions.add(tPartition);
            // tablet
            int quorum = olapTable.getPartitionInfo().getReplicaAllocation(partition.getId()).getTotalReplicaNum() / 2
                    + 1;
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    // we should ensure the replica backend is alive
                    // otherwise, there will be a 'unknown node id, id=xxx' error for stream load
                    // BE id -> path hash
                    Multimap<Long, Long> bePathsMap;
                    try {
                        if (Config.isCloudMode() && request.isSetBeEndpoint()) {
                            bePathsMap = ((CloudTablet) tablet)
                                    .getNormalReplicaBackendPathMapCloud(request.be_endpoint);
                        } else {
                            bePathsMap = tablet.getNormalReplicaBackendPathMap();
                        }
                    } catch (UserException ex) {
                        errorStatus.setErrorMsgs(Lists.newArrayList(ex.getMessage()));
                        result.setStatus(errorStatus);
                        LOG.warn("send create partition error status: {}", result);
                        return result;
                    }
                    if (bePathsMap.keySet().size() < quorum) {
                        LOG.warn("auto go quorum exception");
                    }
                    if (request.isSetWriteSingleReplica() && request.isWriteSingleReplica()) {
                        Long[] nodes = bePathsMap.keySet().toArray(new Long[0]);
                        Random random = new SecureRandom();
                        Long masterNode = nodes[random.nextInt(nodes.length)];
                        Multimap<Long, Long> slaveBePathsMap = bePathsMap;
                        slaveBePathsMap.removeAll(masterNode);
                        tablets.add(new TTabletLocation(tablet.getId(),
                                Lists.newArrayList(Sets.newHashSet(masterNode))));
                        slaveTablets.add(new TTabletLocation(tablet.getId(),
                                Lists.newArrayList(slaveBePathsMap.keySet())));
                    } else {
                        tablets.add(new TTabletLocation(tablet.getId(), Lists.newArrayList(bePathsMap.keySet())));
                    }
                }
            }
        }
        result.setPartitions(partitions);
        result.setTablets(tablets);
        result.setSlaveTablets(slaveTablets);

        // build nodes
        List<TNodeInfo> nodeInfos = Lists.newArrayList();
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        for (Long id : systemInfoService.getAllBackendByCurrentCluster(false)) {
            Backend backend = systemInfoService.getBackend(id);
            nodeInfos.add(new TNodeInfo(backend.getId(), 0, backend.getHost(), backend.getBrpcPort()));
        }
        result.setNodes(nodeInfos);
        result.setStatus(new TStatus(TStatusCode.OK));
        if (LOG.isDebugEnabled()) {
            LOG.debug("send create partition result: {}", result);
        }
        return result;
    }

    @Override
    public TReplacePartitionResult replacePartition(TReplacePartitionRequest request) throws TException {
        LOG.info("Receive replace partition request: {}", request);
        long dbId = request.getDbId();
        long tableId = request.getTableId();
        List<Long> reqPartitionIds = request.getPartitionIds();
        long taskGroupId = request.getOverwriteGroupId();
        TReplacePartitionResult result = new TReplacePartitionResult();
        TStatus errorStatus = new TStatus(TStatusCode.RUNTIME_ERROR);
        if (!Env.getCurrentEnv().isMaster()) {
            errorStatus.setStatusCode(TStatusCode.NOT_MASTER);
            errorStatus.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.warn("failed to replace Partition: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            errorStatus.setErrorMsgs(Lists.newArrayList(String.format("dbId=%d is not exists", dbId)));
            result.setStatus(errorStatus);
            LOG.warn("send replace partition error status: {}", result);
            return result;
        }

        Table table = db.getTable(tableId).get();
        if (table == null) {
            errorStatus.setErrorMsgs(
                    (Lists.newArrayList(String.format("dbId=%d tableId=%d is not exists", dbId, tableId))));
            result.setStatus(errorStatus);
            LOG.warn("send replace partition error status: {}", result);
            return result;
        }

        if (!(table instanceof OlapTable)) {
            errorStatus.setErrorMsgs(
                    Lists.newArrayList(String.format("dbId=%d tableId=%d is not olap table", dbId, tableId)));
            result.setStatus(errorStatus);
            LOG.warn("send replace partition error status: {}", result);
            return result;
        }

        OlapTable olapTable = (OlapTable) table;
        InsertOverwriteManager overwriteManager = Env.getCurrentEnv().getInsertOverwriteManager();
        ReentrantLock taskLock = overwriteManager.getLock(taskGroupId);
        if (taskLock == null) {
            errorStatus.setErrorMsgs(Lists
                    .newArrayList(new String("cannot find task group " + taskGroupId + ", maybe already failed.")));
            result.setStatus(errorStatus);
            LOG.warn("send create partition error status: {}", result);
            return result;
        }

        ArrayList<Long> resultPartitionIds = new ArrayList<>(); // [1 2 5 6] -> [7 8 5 6]
        ArrayList<Long> pendingPartitionIds = new ArrayList<>(); // pending: [1 2]
        ArrayList<Long> newPartitionIds = new ArrayList<>(); // requested temp partition ids. for [7 8]
        boolean needReplace = false;
        try {
            taskLock.lock();
            // double check lock. maybe taskLock is not null, but has been removed from the Map. means the task failed.
            if (overwriteManager.getLock(taskGroupId) == null) {
                errorStatus.setErrorMsgs(Lists
                        .newArrayList(new String("cannot find task group " + taskGroupId + ", maybe already failed.")));
                result.setStatus(errorStatus);
                LOG.warn("send create partition error status: {}", result);
                return result;
            }

            // we dont lock the table. other thread in this txn will be controled by taskLock.
            // if we have already replaced, dont do it again, but acquire the recorded new partition directly.
            // if not by this txn, just let it fail naturally is ok.
            needReplace = overwriteManager.tryReplacePartitionIds(taskGroupId, reqPartitionIds, resultPartitionIds);
            // request: [1 2 3 4] result: [1 2 5 6] means ONLY 1 and 2 need replace.
            if (needReplace) {
                // names for [1 2]. if another txn dropped origin partitions, we will fail here. return the exception.
                // that's reasonable because we want iot-auto-detect txn has as less impact as possible. so only when we
                // detected the conflict in this RPC, we will fail the txn. it allows more concurrent transactions.
                List<String> pendingPartitionNames = olapTable.getEqualPartitionNames(reqPartitionIds,
                                resultPartitionIds);
                for (String name : pendingPartitionNames) {
                    pendingPartitionIds.add(olapTable.getPartition(name).getId()); // put [1 2]
                }

                // names for [7 8]
                List<String> newTempNames = InsertOverwriteUtil
                        .generateTempPartitionNames(pendingPartitionNames);
                // a task means one time insert overwrite
                long taskId = overwriteManager.registerTask(dbId, tableId, newTempNames);
                overwriteManager.registerTaskInGroup(taskGroupId, taskId);
                InsertOverwriteUtil.addTempPartitions(olapTable, pendingPartitionNames, newTempNames);
                // now temp partitions are bumped up and use new names. we get their ids and record them.
                for (String newPartName : newTempNames) {
                    newPartitionIds.add(olapTable.getPartition(newPartName).getId()); // put [7 8]
                }
                overwriteManager.recordPartitionPairs(taskGroupId, pendingPartitionIds, newPartitionIds);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("partition replacement: ");
                    for (int i = 0; i < pendingPartitionIds.size(); i++) {
                        LOG.debug("[" + pendingPartitionIds.get(i) + " - " + pendingPartitionNames.get(i) + ", "
                                + newPartitionIds.get(i) + " - " + newTempNames.get(i) + "], ");
                    }
                }
            }
        } catch (DdlException | RuntimeException ex) {
            errorStatus.setErrorMsgs(Lists.newArrayList(ex.getMessage()));
            result.setStatus(errorStatus);
            LOG.warn("send create partition error status: {}", result);
            return result;
        } finally {
            taskLock.unlock();
        }

        // result: [1 2 5 6], make it [7 8 5 6]
        int idx = 0;
        if (needReplace) {
            for (int i = 0; i < reqPartitionIds.size(); i++) {
                if (reqPartitionIds.get(i).equals(resultPartitionIds.get(i))) {
                    resultPartitionIds.set(i, newPartitionIds.get(idx++));
                }
            }
        }
        if (idx != newPartitionIds.size()) {
            errorStatus.addToErrorMsgs("changed partition number " + idx + " is not correct");
            result.setStatus(errorStatus);
            LOG.warn("send create partition error status: {}", result);
            return result;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("replace partition origin ids: ["
                    + String.join(", ", reqPartitionIds.stream().map(String::valueOf).collect(Collectors.toList()))
                    + ']');
            LOG.debug("replace partition result ids: ["
                    + String.join(", ", resultPartitionIds.stream().map(String::valueOf).collect(Collectors.toList()))
                    + ']');
        }

        // build partition & tablets. now all partitions in allReqPartNames are replaced an recorded.
        // so they won't be changed again. if other transaction changing it. just let it fail.
        List<TOlapTablePartition> partitions = new ArrayList<>();
        List<TTabletLocation> tablets = new ArrayList<>();
        List<TTabletLocation> slaveTablets = new ArrayList<>();
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        for (long partitionId : resultPartitionIds) {
            Partition partition = olapTable.getPartition(partitionId);
            TOlapTablePartition tPartition = new TOlapTablePartition();
            tPartition.setId(partition.getId());

            // set partition keys
            int partColNum = partitionInfo.getPartitionColumns().size();
            try {
                OlapTableSink.setPartitionKeys(tPartition, partitionInfo.getItem(partition.getId()), partColNum);
            } catch (UserException ex) {
                errorStatus.setErrorMsgs(Lists.newArrayList(ex.getMessage()));
                result.setStatus(errorStatus);
                LOG.warn("send replace partition error status: {}", result);
                return result;
            }
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                        index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
                tPartition.setNumBuckets(index.getTablets().size());
            }
            tPartition.setIsMutable(olapTable.getPartitionInfo().getIsMutable(partition.getId()));
            partitions.add(tPartition);
            // tablet
            int quorum = olapTable.getPartitionInfo().getReplicaAllocation(partition.getId()).getTotalReplicaNum() / 2
                    + 1;
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    // we should ensure the replica backend is alive
                    // otherwise, there will be a 'unknown node id, id=xxx' error for stream load
                    // BE id -> path hash
                    Multimap<Long, Long> bePathsMap;
                    try {
                        if (Config.isCloudMode() && request.isSetBeEndpoint()) {
                            bePathsMap = ((CloudTablet) tablet)
                                    .getNormalReplicaBackendPathMapCloud(request.be_endpoint);
                        } else {
                            bePathsMap = tablet.getNormalReplicaBackendPathMap();
                        }
                    } catch (UserException ex) {
                        errorStatus.setErrorMsgs(Lists.newArrayList(ex.getMessage()));
                        result.setStatus(errorStatus);
                        LOG.warn("send create partition error status: {}", result);
                        return result;
                    }
                    if (bePathsMap.keySet().size() < quorum) {
                        LOG.warn("auto go quorum exception");
                    }
                    if (request.isSetWriteSingleReplica() && request.isWriteSingleReplica()) {
                        Long[] nodes = bePathsMap.keySet().toArray(new Long[0]);
                        Random random = new SecureRandom();
                        Long masterNode = nodes[random.nextInt(nodes.length)];
                        Multimap<Long, Long> slaveBePathsMap = bePathsMap;
                        slaveBePathsMap.removeAll(masterNode);
                        tablets.add(new TTabletLocation(tablet.getId(),
                                Lists.newArrayList(Sets.newHashSet(masterNode))));
                        slaveTablets.add(new TTabletLocation(tablet.getId(),
                                Lists.newArrayList(slaveBePathsMap.keySet())));
                    } else {
                        tablets.add(new TTabletLocation(tablet.getId(), Lists.newArrayList(bePathsMap.keySet())));
                    }
                }
            }
        }
        result.setPartitions(partitions);
        result.setTablets(tablets);
        result.setSlaveTablets(slaveTablets);

        // build nodes
        List<TNodeInfo> nodeInfos = Lists.newArrayList();
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        for (Long id : systemInfoService.getAllBackendByCurrentCluster(false)) {
            Backend backend = systemInfoService.getBackend(id);
            nodeInfos.add(new TNodeInfo(backend.getId(), 0, backend.getHost(), backend.getBrpcPort()));
        }
        result.setNodes(nodeInfos);

        // successfully return
        result.setStatus(new TStatus(TStatusCode.OK));
        if (LOG.isDebugEnabled()) {
            LOG.debug("send replace partition result: {}", result);
        }
        return result;
    }

    public TGetMetaResult getMeta(TGetMetaRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive get meta request: {}", request);
        }

        TGetMetaResult result = new TGetMetaResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get beginTxn: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        try {
            result = getMetaImpl(request, clientAddr);
        } catch (UserException e) {
            LOG.warn("failed to get meta: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
        }
        return result;
    }

    private TGetMetaResult getMetaImpl(TGetMetaRequest request, String clientIp)
            throws Exception {
        // Step 1: check fields
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }

        // Step 2: check auth
        TGetMetaResult result = new TGetMetaResult();
        result.setStatus(new TStatus(TStatusCode.OK));
        Database db = null;
        List<Table> tables = null;

        if (Strings.isNullOrEmpty(request.getToken())) {
            TGetMetaDB getMetaDb = request.getDb();

            if (getMetaDb.isSetId()) {
                db = Env.getCurrentInternalCatalog().getDbNullable(getMetaDb.getId());
            } else if (getMetaDb.isSetName()) {
                db = Env.getCurrentInternalCatalog().getDbNullable(getMetaDb.getName());
            }

            if (db == null) {
                LOG.warn("db not found {}", getMetaDb);
                return result;
            }

            if (getMetaDb.isSetTables()) {
                tables = Lists.newArrayList();
                List<TGetMetaTable> getMetaTables = getMetaDb.getTables();
                for (TGetMetaTable getMetaTable : getMetaTables) {
                    Table table = null;
                    if (getMetaTable.isSetId()) {
                        table = db.getTableNullable(getMetaTable.getId());
                    } else {
                        table = db.getTableNullable(getMetaTable.getName());
                    }

                    if (table == null) {
                        // Since Database.getTableNullable is lock-free, we need to take lock and check again,
                        // to ensure the visibility of the table.
                        db.readLock();
                        try {
                            if (getMetaTable.isSetId()) {
                                table = db.getTableNullable(getMetaTable.getId());
                            } else {
                                table = db.getTableNullable(getMetaTable.getName());
                            }
                        } finally {
                            db.readUnlock();
                        }
                    }

                    if (table == null) {
                        LOG.warn("table not found {}", getMetaTable);
                        continue;
                    }

                    tables.add(table);
                }
            }

            if (tables == null) {
                checkDbPasswordAndPrivs(request.getUser(), request.getPasswd(), db.getFullName(), clientIp,
                        PrivPredicate.SELECT);
            } else {
                List<String> tableList = Lists.newArrayList();
                for (Table table : tables) {
                    tableList.add(table.getName());
                }
                checkPasswordAndPrivs(request.getUser(), request.getPasswd(), db.getFullName(), tableList,
                        clientIp,
                        PrivPredicate.SELECT);
            }
        }

        // Step 3: get meta
        try {
            return Env.getMeta(db, tables);
        } catch (Throwable e) {
            throw e;
        }
    }

    @Override
    public TGetColumnInfoResult getColumnInfo(TGetColumnInfoRequest request) {
        TGetColumnInfoResult result = new TGetColumnInfoResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        long dbId = request.getDbId();
        long tableId = request.getTableId();
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to getColumnInfo: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            status.setStatusCode(TStatusCode.NOT_FOUND);
            status.setErrorMsgs(Lists.newArrayList(String.format("dbId=%d is not exists", dbId)));
            return result;
        }
        Table table = db.getTableNullable(tableId);
        if (table == null) {
            status.setStatusCode(TStatusCode.NOT_FOUND);
            status.setErrorMsgs(
                    (Lists.newArrayList(String.format("dbId=%d tableId=%d is not exists", dbId, tableId))));
            return result;
        }
        List<TColumnInfo> columnsResult = Lists.newArrayList();
        for (Column column : table.getBaseSchema(true)) {
            final TColumnInfo info = new TColumnInfo();
            info.setColumnName(column.getName());
            info.setColumnId(column.getUniqueId());
            columnsResult.add(info);
        }
        result.setColumns(columnsResult);
        return result;
    }

    public TGetBackendMetaResult getBackendMeta(TGetBackendMetaRequest request) {
        String clientAddr = getClientAddrAsString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive get backend meta request: {}", request);
        }

        TGetBackendMetaResult result = new TGetBackendMetaResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get beginTxn: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        try {
            result = getBackendMetaImpl(request, clientAddr);
        } catch (UserException e) {
            LOG.warn("failed to get backend meta: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
        }

        return result;
    }

    private TGetBackendMetaResult getBackendMetaImpl(TGetBackendMetaRequest request, String clientAddr)
            throws UserException {
        // Step 1: Check fields
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }

        // Step 2: check auth
        checkPassword(request.getUser(), request.getPasswd(), clientAddr);

        // TODO: check getBackendMeta privilege, which privilege should be checked?

        // Step 3: get meta
        try {
            TGetBackendMetaResult result = new TGetBackendMetaResult();
            result.setStatus(new TStatus(TStatusCode.OK));

            final SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
            List<Backend> backends = systemInfoService.getAllBackendsByAllCluster().values().asList();

            for (Backend backend : backends) {
                TBackend tBackend = new TBackend();
                tBackend.setId(backend.getId());
                tBackend.setHost(backend.getHost());
                tBackend.setHttpPort(backend.getHttpPort());
                tBackend.setBrpcPort(backend.getBrpcPort());
                tBackend.setBePort(backend.getBePort());
                tBackend.setIsAlive(backend.isAlive());
                result.addToBackends(tBackend);
            }

            return result;
        } catch (Throwable e) {
            throw e;
        }
    }

    class TableStats {
        public long updatedRowCount;
    }

    public TStatus reportCommitTxnResult(TReportCommitTxnResultRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        // FE only has one master, this should not be a problem
        if (!Env.getCurrentEnv().isMaster()) {
            LOG.error("failed to handle load stats report: not master, backend:{}",
                    clientAddr);
            return new TStatus(TStatusCode.NOT_MASTER);
        }

        LOG.info("receive load stats report request: {}, backend: {}, dbId: {}, txnId: {}, label: {}",
                request, clientAddr, request.getDbId(), request.getTxnId(), request.getLabel());

        try {
            byte[] receivedProtobufBytes = request.getPayload();
            if (receivedProtobufBytes == null || receivedProtobufBytes.length <= 0) {
                return new TStatus(TStatusCode.INVALID_ARGUMENT);
            }
            CommitTxnResponse commitTxnResponse = CommitTxnResponse.parseFrom(receivedProtobufBytes);
            Env.getCurrentGlobalTransactionMgr().afterCommitTxnResp(commitTxnResponse);
        } catch (InvalidProtocolBufferException e) {
            // Handle the exception, log it, or take appropriate action
            e.printStackTrace();
        }

        return new TStatus(TStatusCode.OK);
    }

    @Override
    public TShowProcessListResult showProcessList(TShowProcessListRequest request) {
        boolean isShowFullSql = false;
        if (request.isSetShowFullSql()) {
            isShowFullSql = request.isShowFullSql();
        }
        UserIdentity userIdentity = UserIdentity.ROOT;
        if (request.isSetCurrentUserIdent()) {
            userIdentity = UserIdentity.fromThrift(request.getCurrentUserIdent());
        }
        String timeZone = VariableMgr.getDefaultSessionVariable().getTimeZone();
        if (request.isSetTimeZone()) {
            timeZone = request.getTimeZone();
        }
        List<List<String>> processList = ExecuteEnv.getInstance().getScheduler()
                .listConnectionForRpc(userIdentity, isShowFullSql, Optional.of(timeZone));
        TShowProcessListResult result = new TShowProcessListResult();
        result.setProcessList(processList);
        return result;
    }

    @Override
    public TShowUserResult showUser(TShowUserRequest request) {
        List<List<String>> userInfo = Env.getCurrentEnv().getAuth().getAllUserInfo();
        TShowUserResult result = new TShowUserResult();
        result.setUserinfoList(userInfo);
        return result;
    }

    public TStatus syncQueryColumns(TSyncQueryColumns request) throws TException {
        Env.getCurrentEnv().getAnalysisManager().mergeFollowerQueryColumns(request.highPriorityColumns,
                request.midPriorityColumns);
        return new TStatus(TStatusCode.OK);
    }

    @Override
    public TFetchRunningQueriesResult fetchRunningQueries(TFetchRunningQueriesRequest request) {
        TFetchRunningQueriesResult result = new TFetchRunningQueriesResult();
        if (!Env.getCurrentEnv().isReady()) {
            result.setStatus(new TStatus(TStatusCode.ILLEGAL_STATE));
            return result;
        }

        List<TUniqueId> runningQueries = Lists.newArrayList();
        List<Coordinator> allCoordinators = QeProcessorImpl.INSTANCE.getAllCoordinators();

        for (Coordinator coordinator : allCoordinators) {
            runningQueries.add(coordinator.getQueryId());
        }

        result.setStatus(new TStatus(TStatusCode.OK));
        result.setRunningQueries(runningQueries);
        return result;
    }

    @Override
    public TFetchRoutineLoadJobResult fetchRoutineLoadJob(TFetchRoutineLoadJobRequest request) {
        TFetchRoutineLoadJobResult result = new TFetchRoutineLoadJobResult();

        if (!Env.getCurrentEnv().isReady()) {
            return result;
        }

        RoutineLoadManager routineLoadManager = Env.getCurrentEnv().getRoutineLoadManager();
        List<TRoutineLoadJob> jobInfos = Lists.newArrayList();
        List<RoutineLoadJob> routineLoadJobs = routineLoadManager.getAllRoutineLoadJobs();
        for (RoutineLoadJob job : routineLoadJobs) {
            TRoutineLoadJob jobInfo = new TRoutineLoadJob();
            jobInfo.setJobId(String.valueOf(job.getId()));
            jobInfo.setJobName(job.getName());
            jobInfo.setCreateTime(job.getCreateTimestampString());
            jobInfo.setPauseTime(job.getPauseTimestampString());
            jobInfo.setEndTime(job.getEndTimestampString());
            String dbName = "";
            String tableName = "";
            try {
                dbName = job.getDbFullName();
                tableName = job.getTableName();
            } catch (MetaNotFoundException e) {
                LOG.warn("Failed to get db or table name for routine load job: {}", job.getId(), e);
            }
            jobInfo.setDbName(dbName);
            jobInfo.setTableName(tableName);
            jobInfo.setState(job.getState().name());
            jobInfo.setCurrentTaskNum(String.valueOf(job.getSizeOfRoutineLoadTaskInfoList()));
            jobInfo.setJobProperties(job.jobPropertiesToJsonString());
            jobInfo.setDataSourceProperties(job.dataSourcePropertiesJsonToString());
            jobInfo.setCustomProperties(job.customPropertiesJsonToString());
            jobInfo.setStatistic(job.getStatistic());
            jobInfo.setProgress(job.getProgress().toJsonString());
            jobInfo.setLag(job.getLag());
            jobInfo.setReasonOfStateChanged(job.getStateReason());
            jobInfo.setErrorLogUrls(Joiner.on(", ").join(job.getErrorLogUrls()));
            jobInfo.setUserName(job.getUserIdentity().getQualifiedUser());
            jobInfo.setCurrentAbortTaskNum(job.getJobStatistic().currentAbortedTaskNum);
            jobInfo.setIsAbnormalPause(job.isAbnormalPause());
            jobInfos.add(jobInfo);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("routine load job infos: {}", jobInfos);
        }
        result.setRoutineLoadJobs(jobInfos);

        return result;
    }
}

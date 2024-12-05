use crate::grpc_wrapper::raw_table_service::execute_data_query::{RawOperationStats, RawQueryPhaseStats, RawQueryStats, RawTableAccessStats};
use std::fmt::Debug;

pub struct QueryStats {
    pub process_cpu_time: std::time::Duration,
    pub total_duration: std::time::Duration,
    pub total_cpu_time: std::time::Duration,
    pub query_plan: String,
    pub query_ast: String,

    pub query_phases: Vec<QueryPhaseStats>,
}


/// Specifies which statistics should be collected during request processing
#[derive(Clone)]
pub enum QueryStatsMode {
    /// Stats collection is disabled
    None,

    /// Aggregated stats of reads, updates and deletes per table    
    Basic,

    /// Add execution stats and plan on top of STATS_COLLECTION_BASIC
    Full,

    /// Detailed execution stats including stats for individual tasks and channels
    Profile,
}


impl From<RawQueryStats> for QueryStats{

    fn from(value: RawQueryStats) -> QueryStats {

        let query_phases = value.query_phases.into_iter().map(Into::into).collect();
        
        Self {
            process_cpu_time: value.process_cpu_time,
            total_duration: value.total_duration,
            total_cpu_time: value.total_cpu_time,
            query_ast: value.query_ast,
            query_plan: value.query_plan,

            query_phases,
        }
    }
}


pub struct QueryPhaseStats {
    pub duration: std::time::Duration,
    pub table_access: Vec<TableAccessStats>,
    pub cpu_time: std::time::Duration,
    pub affected_shards: u64,
    pub literal_phase: bool,
}

impl From<RawQueryPhaseStats> for QueryPhaseStats {
   
    fn from(value: RawQueryPhaseStats) -> Self {
        let table_access: Vec<_> = value.table_access.into_iter().map(Into::into).collect();

        Self {

            duration: value.duration,
            table_access,
            cpu_time: value.cpu_time,
            affected_shards: value.affected_shards,
            literal_phase: value.literal_phase,
        }
    }
}


pub struct TableAccessStats {
    pub name: String,
    pub reads: Option<OperationStats>,
    pub updates: Option<OperationStats>,
    pub deletes: Option<OperationStats>,
    pub partitions_count: u64
}




impl From<RawTableAccessStats> for TableAccessStats {
    fn from(value: RawTableAccessStats) -> Self {
        

        let reads= value.reads.map(Into::into);
        let updates= value.updates.map(Into::into);
        let deletes= value.deletes.map(Into::into);

        

        Self {
            name: value.name,
            reads,
            updates,
            deletes,
            partitions_count: value.partitions_count,
        }
    }
}



impl From<RawOperationStats> for OperationStats {
    fn from(value: RawOperationStats) -> Self {
        Self {
            rows: value.rows,
            bytes: value.bytes,
        }
    }
}


pub struct OperationStats {
    pub rows: u64,
    pub bytes: u64,
}


impl QueryStats{
    pub fn rows_affected(&self) -> u64 {
        self.query_phases.iter().fold(0, |acc, r| acc + r.rows_affected())
    }
}

impl QueryPhaseStats{
    pub fn rows_affected(&self) -> u64 {
        self.table_access.iter().fold(0, |acc, r| acc + r.rows_affected())
    }
}

impl TableAccessStats{
    pub fn rows_affected(&self) -> u64 {
        Self::_rows_affected(&self.reads) + Self::_rows_affected(&self.updates) + Self::_rows_affected(&self.deletes)
    }
    fn _rows_affected(stats: &Option<OperationStats>) -> u64 {
        stats.as_ref().map(|x|x.rows).unwrap_or(0)
    }
}

impl Debug for QueryStats{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\n=== Query Stats: ===")?;
        writeln!(f,"process_cpu_time: {:?}",self.process_cpu_time)?;  
        writeln!(f,"total_duration: {:?}",self.total_duration)?;
        writeln!(f,"total_cpu_time: {:?}",self.total_cpu_time)?;
        writeln!(f,"AST: {}",self.query_ast)?;
        writeln!(f,"Plan: {}",self.query_plan)?;
        writeln!(f,"Phases ({}):",self.query_phases.len())?;

        for phase in &self.query_phases {
            writeln!(f, "--------------------")?;
            writeln!(f,"{:?}",phase)?;
        }

        Ok(())
    }
}

impl Debug for QueryPhaseStats{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f,"duration: {:?}",self.duration)?;
        writeln!(f,"cpu_time: {:?}",self.cpu_time)?;
        writeln!(f,"affected_shards: {}",self.affected_shards)?;
        writeln!(f,"literal_phase: {}",self.literal_phase)?;

        writeln!(f,"Tables ({}):",self.table_access.len())?;
        for table in &self.table_access {
            writeln!(f,"{:?}",table)?;
        }
        Ok(())
    }
}

impl Debug for TableAccessStats{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f,"name: {}",self.name)?;
        writeln!(f,"\t[ROWS] reads: {}, updates: {}, deletes: {}",
            self.reads.as_ref().map(|x|x.rows).unwrap_or(0),
            self.updates.as_ref().map(|x|x.rows).unwrap_or(0),
            self.deletes.as_ref().map(|x|x.rows).unwrap_or(0))?;

        writeln!(f,"\t[BYTES] reads: {}, updates: {}, deletes: {}",
            self.reads.as_ref().map(|x|x.bytes).unwrap_or(0),
            self.updates.as_ref().map(|x|x.bytes).unwrap_or(0),
            self.deletes.as_ref().map(|x|x.bytes).unwrap_or(0))?;
        
        Ok(())
    }
}
#[macro_use]
extern crate simple_log;
extern crate tar;
extern crate flate2;

use sqlx::{Executor, MySqlPool, mysql,Row,ConnectOptions};
use sqlx::mysql::MySqlConnectOptions;
use sqlx::mysql::MySqlSslMode;
use std::str::FromStr;
use std::time::Instant;
use std::time::Duration;
use std::env;
use std::path::Path;
use std::fs;
// use std::io::prelude::*;
// use simple_log::log::{info, error};
use simple_log::LogConfig;
use serde_json::Value;

use std::io::prelude::*;
use std::fs::File;
use tar::Archive;
use flate2::read::GzDecoder;


#[derive( Clone)]
struct Table{
    name: String,
    ddl: String,
    fields: String,
    meta: String,
    data_files: Vec<String>,
    rows: u64,
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error>{
    let config = r#"
    {
        "path":"./log/nb3_import.log",
        "level":"info",
        "size":10,
        "out_kind":["console","file"],
        "roll_count":10,
        "time_format":"%H:%M:%S.%f"
    }"#;
    let log_config: LogConfig = serde_json::from_str(config).unwrap();

    simple_log::new(log_config).unwrap();//init log
    let args: Vec<String> = env::args().collect();
    let  mut tables: Vec<Table> = Vec::<Table>::new();
    if args.len() > 1 {
        // 解压nb3文件
        tables = extract(&*args[1]);
    }
    if tables.len() > 0 {
        println!("解析nb3文件成功！,需要导入的表有{}个",tables.len());
    } else {
        println!("解析nb3文件失败！");
        return Ok(());
    }
    let mut url = String::new();
    if args.len() > 2 {
        url = String::from(args[2].as_str());
    } else {
        info!("nb3文件已成功解析，请输入数据库连接字符串,格式: mysql://user:pass@host:port/dbname,例如: mysql://hams:hams@localhost:3306/ylj");
        std::io::stdin().read_line(&mut url).unwrap();
    }
    let pool :MySqlPool ;
    loop {
        let mut opts = MySqlConnectOptions::from_str(url.as_str()).unwrap();
        opts = opts.ssl_mode(MySqlSslMode::Disabled);
        opts.disable_statement_logging();
        let c =  mysql::MySqlPoolOptions::new().min_connections(SIZE).max_connections(SIZE).connect_timeout(Duration::from_secs(60)).idle_timeout(Duration::from_secs(10))
        .connect_with(opts).await;
        match c {
            Ok(r) => {
                pool = r;
                info!("数据库连接成功: {} 开始处理", url);
                break;
            },
            Err(e) => {
                info!("字符串:{} 连接失败,详细错误信息:{:?},\n请重新输入连接字符串",url,e);
                url.clear();
                std::io::stdin().read_line(&mut url).unwrap();
            }
        }
    }
    let now = Instant::now();
    const SIZE:u32 = 100;
    info!("数据库连接池 is : {:?}", pool);
    // 循环每个table处理
    let mut i = 0;
    let length = tables.len();
    while i < length {
        let mut j = 0;
        let mut handles2 = Vec::with_capacity(10);
        while j < 10 && i < length {
            handles2.push(tokio::spawn(execute(tables[i].clone(),pool.clone())));
            i+=1;
            j+=1;
        }
        for handle in handles2 {
            let _r = handle.await;
        }
    }
    pool.close().await;
    info!("程序结束 总耗时：{} ms", now.elapsed().as_secs());
    Ok(())
}
async fn execute(table: Table, pool2:MySqlPool ){
    // 创建文件夹 _nb3
    fs::create_dir_all("_nb3").unwrap();
    let drop_sql = format!("drop table {}",table.name);
    pool2.execute(sqlx::query(&drop_sql)).await.unwrap();
    // 建表 
    pool2.execute(sqlx::query(&table.ddl)).await.unwrap();
    for j in 0..table.data_files.len(){
        // 读取文本内容
        let p = format!("./_nb3/{}",&table.data_files[j]);
        let mut gz = GzDecoder::new(File::open(&p).unwrap());
        let mut data = String::new();
        gz.read_to_string(&mut data).unwrap();
        // 处理特殊字符 "\u{1e}"
        while data.contains("\u{1e}") {
            data = data.replace("\u{1e}", ",");
        }
        let insert_sql = format!("insert into {} ({}) values {} ",table.name,table.fields,data);
        let _r = pool2.execute(sqlx::query(&insert_sql)).await;
        match _r {
            Ok(r) => {
            },
            Err(_e) => {
                info!("sql出错:{} ",insert_sql);
            }
        }
        println!("插入{}中数据到表{}",p,table.name);
    }
    // 查询count 确认数据行数
    let count_sql = format!("select count(1) as count from {}",table.name);
    let row = sqlx::query(&count_sql).fetch_one(&pool2).await.unwrap();
    let count:u32 = row.get_unchecked("count");
    info!("{}表数据处理完成,共{}行,导入{}行",table.name,table.rows,count);
    
}
fn extract(file_path:&str )->Vec<Table>{
    let file = File::open(file_path).unwrap();
    let _a = Archive::new(file).unpack("_nb3").unwrap();
    let mut tables = Vec::new();
    let meta_path = Path::new("./_nb3/meta.json");
    let v: Value = serde_json::from_str(&fs::read_to_string(meta_path).unwrap()).unwrap();
    let objects = v["Objects"].as_array().unwrap();
    for obj in objects {
        if obj["Type"].as_str().unwrap() == "Table" {
            let name = obj["Name"].as_str().unwrap().to_string();
            let rows= obj["Rows"].as_str().unwrap().to_string().parse().unwrap();
            let meta = obj["Metadata"]["Filename"].as_str().unwrap().to_string();
            // 处理数据文件
            let table_meta = format!("./_nb3/{}",meta);
            println!("处理表{} JSON信息 对应gz文件{}",name,table_meta);
            let meta = table_meta.replace(".gz","");
            let tar_gz = File::open(table_meta).unwrap();
            let mut gz = GzDecoder::new(tar_gz);
            let mut s = String::new();
            gz.read_to_string(&mut s).unwrap();
            let t: Value = serde_json::from_str(&s).unwrap();
            let ddl = t["DDL"].as_str().unwrap().to_string();
            let mut data_files = Vec::new();
            let data = t["Data"].as_array().unwrap();
            for d in data {
                data_files.push(d["Filename"].as_str().unwrap().to_string());
            }
            let fields_array = t["Fields"].as_array().unwrap();
            let mut fields_vec = Vec::new();
            for fa in fields_array {
                fields_vec.push(fa.as_str().unwrap().to_string());
            }
            let fields = fields_vec.join(",");
            let table = Table{
                name,
                rows,
                meta,
                ddl,
                fields,
                data_files,
            };  
            tables.push(table);
        }
    }
    return tables;
}
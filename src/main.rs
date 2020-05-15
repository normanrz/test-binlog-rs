use futures_core;
use futures_util::stream::StreamExt;
use mysql_async::prelude::*;
use mysql_common;
use std::pin::Pin;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

async fn get_binlog_stream(
    url: &str,
    server_id: u32,
) -> Result<
    Pin<
        Box<
            dyn futures_core::stream::Stream<
                Item = mysql_async::Result<mysql_common::binlog::Event>,
            >,
        >,
    >,
> {
    let opts = mysql_async::Opts::from_url(url)?;
    let mut conn = mysql_async::Conn::new(opts).await?;
    let (filename, position): (String, u64) = "SHOW BINARY LOGS".first(&mut conn).await?.unwrap();

    let binlog_stream = conn
        .get_binlog_stream(
            mysql_async::BinlogRequest::new(server_id)
                .with_filename(filename)
                .with_pos(position),
        )
        .await?;
    return Ok(binlog_stream);
}

#[tokio::main]
async fn main() -> Result<()> {
    let database_url = "mysql://root:password@localhost/mysql";

    tokio::spawn(async move {
        let mut i = 0;
        let mut binlog_stream = get_binlog_stream(database_url, 12).await.unwrap();
        while let Ok(event) = binlog_stream.next().await.unwrap() {
            let event_data = event.read_data().unwrap().unwrap();
            match event_data {
                mysql_common::binlog::EventData::WriteRowsEventV1(event_data) => {
                    println!("{:?}", event_data);
                    i += 1;
                }
                _ => {}
            }
            // println!("{:?}", event.header.event_type.get().unwrap());
            if i > 100 {
                break;
            }
        }
    });

    Ok(())
}

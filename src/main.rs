extern crate actix_web;
use actix_web::{get, App, HttpResponse, HttpServer, Responder};
// use std::env;
// use serde_json::value::Value;
use std::fs;
// use aws_sdk_s3::{config, Client, Error};
// use polars::prelude::*;
// use datafusion_objectstore_s3::object_store::s3::S3FileSystem;
// use std::sync::Arc;
// use datafusion::datasource::listing::*;
// use datafusion::prelude::SessionContext;



#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Welcome to the Premier Soccer Analyzer!")
}

//function that plots the team results
#[get("/team_results")]
async fn team_result_plot() -> Result<HttpResponse, actix_web::error::ParseError> {
    // async fn team_result_plot() -> impl Responder {
    const TEAM_RES: &str = "processed-data/model_res_teams.csv";
    const TEAM_RES_PNG: &str = "team_res.png";
    final_project::plot_res(TEAM_RES,TEAM_RES_PNG).await;

    // run the plot function and show plot on the actix server
    let image_data = fs::read(TEAM_RES_PNG)?;
    Ok(HttpResponse::Ok()
        .content_type("image/png")
        .body(image_data))

    // HttpResponse::Ok().body("Other test!")
}

//function that plots the player results
#[get("/player_results")]
async fn player_result_plot() -> Result<HttpResponse, actix_web::error::ParseError> {
// async fn player_result_plot() -> impl Responder {
    const PLAY_RES: &str = "processed-data/model_res_players.csv";
    const PLAY_RES_PNG: &str = "player_res.png";
    final_project::plot_res(PLAY_RES,PLAY_RES_PNG).await;

    // run the plot function and show plot on the actix server
    let image_data = fs::read(PLAY_RES_PNG)?;
    Ok(HttpResponse::Ok()
        .content_type("image/png")
        .body(image_data))
    // HttpResponse::Ok().body("Test!")
}


// #[get("/team/{team_name}")]
// async fn team() -> impl Responder {
//     HttpResponse::Ok().body("Hello!")
// }

// #[get("/player/{player_name}")]
// async fn player() -> impl Responder {
//     HttpResponse::Ok().body("Other test!")
// }


// #[actix_web::main]
// async fn main() -> std::io::Result<()> {
//     //add a print message to the console that the service is running
//     println!("Starting service...");
//     HttpServer::new(|| App::new().service(team).service(player))
//         .bind("0.0.0.0:8080")?
//         .run()
//         .await
// }


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // let AWS_S3_BUCKET = env::var("AWS_S3_BUCKET").expect("AWS_S3_BUCKET must be set");
    // let AWS_ACCESS_KEY_ID = env::var("AWS_ACCESS_KEY_ID");
    // let AWS_SECRET_ACCESS_KEY = env::var("AWS_SECRET_ACCESS_KEY");
    // let config = aws_config::from_env().region("us-east-1").load().await;
    // let client = Client::new(&config);


    // let result = client
    //    .get_object()
    //    .bucket(AWS_S3_BUCKET)
    //    .key("raw-data/tags2name.csv")
    //    .send()
    //    .await
    //    .expect("Failed to get object");

    // let bytes = result.body.collect().await.unwrap();
    // let bytes = bytes.into_bytes();

    // let cursor = std::io::Cursor::new(bytes);

    // let df = CsvReader::new(cursor).finish().unwrap();

    // println!("{:?}", df);


    
    // TRY WITH DATAFUSION
    // let s3_file_system = Arc::new(S3FileSystem::default().await);

    // let filename = "s3://data/alltypes_plain.snappy.parquet";


    // let config = datafusion::datasource::listing::ListingTableConfig::new(s3_file_system, filename).infer().await?;

    // let table = ListingTable::try_new(config)?;

    // let mut ctx = SessionContext::new();

    // ctx.register_table("tbl", Arc::new(table))?;

    // let df = ctx.sql("SELECT * FROM tbl").await?;
    // df.show();

    HttpServer::new(|| {
        App::new()
            .service(hello)
            .service(team_result_plot)
            .service(player_result_plot)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await

}



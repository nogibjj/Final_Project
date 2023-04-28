extern crate actix_web;
use actix_web::{get, App, HttpResponse, HttpServer, Responder};

// use std::fs;
// use aws_sdk_s3::Client;
use std::env;

use aws_sdk_s3::types::{
    CompressionType, CsvInput, CsvOutput, ExpressionType, FileHeaderInfo, InputSerialization,
    OutputSerialization, SelectObjectContentEventStream,
};
use aws_sdk_s3::Client;

use polars::prelude::*;




#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Welcome to the Premier Soccer Analyzer!")
}

//function that plots the team results
#[get("/team_results")]
// async fn team_result_plot() -> Result<HttpResponse, actix_web::error::ParseError> {
    async fn team_result_plot() -> impl Responder {
    // const TEAM_RES: &str = "processed-data/model_res_teams.csv";
    // const TEAM_RES_PNG: &str = "team_res.png";
    // final_project::plot_res(TEAM_RES,TEAM_RES_PNG).await;

    // // run the plot function and show plot on the actix server
    // let image_data = fs::read(TEAM_RES_PNG)?;
    // Ok(HttpResponse::Ok()
    //     .content_type("image/png")
    //     .body(image_data))

    HttpResponse::Ok().body("Other test!")
}

//function that plots the player results
#[get("/player_results")]
// async fn player_result_plot() -> Result<HttpResponse, actix_web::error::ParseError> {
async fn player_result_plot() -> impl Responder {
    // const PLAY_RES: &str = "processed-data/model_res_players.csv";
    // const PLAY_RES_PNG: &str = "player_res.png";
    // final_project::plot_res(PLAY_RES,PLAY_RES_PNG).await;

    // // run the plot function and show plot on the actix server
    // let image_data = fs::read(PLAY_RES_PNG)?;
    // Ok(HttpResponse::Ok()
    //     .content_type("image/png")
    //     .body(image_data))
    HttpResponse::Ok().body("Test!")
}


// #[get("/team/{team_name}")]
// async fn team() -> impl Responder {
//     HttpResponse::Ok().body("Hello!")
// }

// #[get("/player/{player_name}")]
// async fn player() -> impl Responder {
//     HttpResponse::Ok().body("Other test!")
// }

async fn get_queried_data(query:String, path:&str, header:FileHeaderInfo) -> Vec<u8>{
    let aws_s3_bucket = env::var("AWS_S3_BUCKET").expect("AWS_S3_BUCKET must be set");
    let config = aws_config::from_env().region("us-east-1").load().await;
    let client = Client::new(&config);

    let mut output = client
        .select_object_content()
        .bucket(aws_s3_bucket)
        .key(path)
        .expression_type(ExpressionType::Sql)
        .expression(query)
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(header)
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .csv(CsvOutput::builder().build())
                .build(),
        )
        .send()
        .await
        .expect("Failed to select object content");


    // get the results of output and print them
    let mut results = Vec::new();


    while let Ok(Some(event)) = output.payload.recv().await {
        match event {
            SelectObjectContentEventStream::Records(records) => {
                let res = records
                .payload()
                .map(|p| std::str::from_utf8(p.as_ref()).unwrap())
                .unwrap_or("")
                .to_string()
                ;

                results.push(res);

            }
            SelectObjectContentEventStream::Stats(stats) => {
                println!("Stats: {:?}", stats.details().unwrap());
            }
            SelectObjectContentEventStream::Progress(progress) => {
                println!("Progress: {:?}", progress.details().unwrap());
            }
            SelectObjectContentEventStream::Cont(_) => {
                println!("Continuation Event");
            }
            SelectObjectContentEventStream::End(_) => {
                println!("End Event");
            }
            otherwise => panic!("Unknown event type: {:?}", otherwise),
        }
    }


    // convert results to bytes
    let mut bytes = Vec::<u8>::new();
    // convert results to bytes and store in test 
    for i in results {
        let temp = i.as_bytes().to_vec();
        bytes.extend(temp);
    }


    bytes
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    


    // let result = client
    //    .get_object()
    //    .bucket(aws_s3_bucket)
    //    .key("raw-data/tags2name.csv")
    //    .send()
    //    .await
    //    .expect("Failed to get object");

    // let bytes = result.body().collect().await.unwrap();
    // let bytes = bytes.into_bytes();

    // print!("{:?}", bytes);

    // QUERIES
    // team name query
    let mut team: String = "SELECT * FROM s3object s WHERE s.\"teamName\" = '".to_owned();
    team.push_str("Arsenal");
    team.push('\'');

    // header query 
    let header: String = "SELECT * FROM s3object s LIMIT 1".to_owned();

    // player name query 
    let player_name = "Lionel Messi";
    // separate first and last name from player_name
    let mut split = player_name.split_whitespace();
    let first_name = split.next().unwrap();
    let last_name = split.next().unwrap();

    // player query
    let mut player: String = "SELECT * FROM s3object s WHERE s.\"firstName\" = '".to_owned();
    player.push_str(first_name);
    player.push_str("' AND s.\"lastName\" = '");
    player.push_str(last_name);
    player.push('\'');



    let test = "processed-data/processed_shots.csv";
    // GET DATA
    let data_bytes = get_queried_data(player, test, FileHeaderInfo::Use).await;
    let header_bytes = get_queried_data(header, test, FileHeaderInfo::None).await;

    // Concat header with data 
    let final_bytes = [header_bytes, data_bytes].concat();

    let cursor = std::io::Cursor::new(final_bytes);
    let final_df = CsvReader::new(cursor).finish().unwrap();


    print!("{:?}", final_df);




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



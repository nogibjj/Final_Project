extern crate actix_web;
use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use final_project::get_queried_data;

use std::fs;

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Welcome to the Premier Soccer Analyzer!")
}

//function that plots the team results
#[get("/team_results")]
async fn team_result_plot() -> Result<HttpResponse, actix_web::error::ParseError> {
    //let start = std::time::Instant::now();
    const TEAM_RES: &str = "processed-data/model_res_teams.csv";
    const TEAM_RES_PNG: &str = "team_res.png";
    final_project::plot_res(TEAM_RES, TEAM_RES_PNG).await;

    // run the plot function and show plot on the actix server
    let image_data = fs::read(TEAM_RES_PNG)?;
    //let end = std::time::Instant::now();
    //println!("Time to create team plot: {:?}", end.duration_since(start));
    Ok(HttpResponse::Ok()
        .content_type("image/png")
        .body(image_data))
}

//function that plots the player results
#[get("/player_results")]
async fn player_result_plot() -> Result<HttpResponse, actix_web::error::ParseError> {
    //let start = std::time::Instant::now();
    const PLAY_RES: &str = "processed-data/model_res_players.csv";
    const PLAY_RES_PNG: &str = "player_res.png";
    final_project::plot_res(PLAY_RES, PLAY_RES_PNG).await;

    // run the plot function and show plot on the actix server
    let image_data = fs::read(PLAY_RES_PNG)?;
    //let end = std::time::Instant::now();
    //println!("Time to create player plot: {:?}", end.duration_since(start));
    Ok(HttpResponse::Ok()
        .content_type("image/png")
        .body(image_data))
}

#[get("/team/{team_name}")]
async fn team_specific_data(team_name: web::Path<String>) -> impl Responder {
    //let start = std::time::Instant::now();
    const TEAM_RES: &str = "processed-data/processed_shots.csv";

    let mut team: String = "SELECT * FROM s3object s WHERE s.\"teamName\" = '".to_owned();
    team.push_str(team_name.to_string().as_str());
    team.push('\'');

    let team_df = get_queried_data(team, TEAM_RES).await;

    let total_goals = team_df
        .column("label_Goal")
        .expect("REASON")
        .sum::<f64>()
        .unwrap();

    let total_shots = team_df.height();

    let total_on_goal = team_df
        .column("label_accurate")
        .expect("REASON")
        .sum::<f64>()
        .unwrap();

    let total_blocked = team_df
        .column("label_blocked")
        .expect("REASON")
        .sum::<f64>()
        .unwrap();

    let total_counter = team_df
        .column("label_counter_attack")
        .expect("REASON")
        .sum::<f64>()
        .unwrap();

    // format string for each team
    let team_string = format!(
        "\nTeam: {team_name}
        Total Goals: {total_goals}
        Total Shots: {total_shots}
        Total On-goal Shots: {total_on_goal}
        Shots off counter attack: {total_counter}
        On-goal shots blocked: {total_blocked}"
    );
    //let end = std::time::Instant::now();
    //println!("Time to create team page: {:?}", end.duration_since(start));

    HttpResponse::Ok().body(team_string)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .service(hello)
            .service(team_result_plot)
            .service(player_result_plot)
            .service(team_specific_data)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}

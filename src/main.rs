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
//     HttpResponse::Ok().body("Test!")
}


#[get("/team/{team_name}")]
async fn team_specific_data(team_name: web::Path<String>) -> impl Responder {
    const TEAM_RES: &str = "processed-data/processed_shots.csv";


    let mut team: String = "SELECT sum(s.label_Goal) AS total_goals, sum(s.label_accurate) / count(*) AS prop_on_goal, sum(s.label_blocked) / count(*) AS prop_blocked, sum(s.label_counter_attack) AS total_counter FROM s3object s WHERE s.\"teamName\" = '".to_owned();
    // let mut team: String = "SELECT sum(s.label_Goal) FROM s3object s WHERE s.\"teamName\" = '".to_owned();

    team.push_str(team_name.to_string().as_str());
    team.push('\'');

    let team_df = get_queried_data(team, TEAM_RES).await;


    // calculate stats for team 
    // FIX
    // add more as desired
    let total_goals = team_df.column("total_goals").unwrap().get(0).unwrap();
    let prop_on_goal = team_df.column("prop_on_goal").unwrap().get(0).unwrap();
    let total_counter = team_df.column("total_counter").unwrap().get(0).unwrap();
    let prop_blocked = team_df.column("prop_blocked").unwrap().get(0).unwrap();


    // format string for each team
    let team_string = format!(
        "\nTeam: {team_name}
        Total goals: {total_goals}
        Proprtion of shots on-goal: {prop_on_goal}
        Prop on-goal shots blocked: {prop_blocked}
        Shots off counter attack: {total_counter}\n"
    );

    //HttpResponse::Ok().body("Hello!")
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



use rocket::routes;
use rocket_dyn_templates::Template;

mod db;
mod ui;

#[rocket::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ui::build().launch().await;
    return Ok(());
}

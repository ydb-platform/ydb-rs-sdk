use crate::db;
use serde::Deserialize;
use std::convert::Infallible;
use std::str::FromStr;
use warp::http::{StatusCode, Uri};
use warp::{Filter, Reply};
use ydb::YdbError;

async fn index() -> Result<impl Reply, Infallible> {
    Ok(warp::reply::html(include_str!("index.html")))
}

#[derive(Deserialize)]
struct GetUrl {
    url: String,
}

async fn get_url(params: GetUrl) -> Result<impl Reply, Infallible> {
    let hash = hashers::fnv::fnv1a32(params.url.as_bytes()).to_string();

    match db::insert(hash.clone(), params.url.clone()).await {
        Ok(_) => Ok(warp::reply::with_status(hash, StatusCode::OK)),
        Err(err) => Ok(warp::reply::with_status(
            format!("failed create short url: {}", err),
            StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

#[derive(Deserialize)]
struct RedirectParams {
    l: String,
}
async fn redirect(params: RedirectParams) -> Result<impl Reply, Infallible> {
    let reply: Box<dyn Reply> = match db::get(params.l).await {
        Ok(url) => match Uri::from_str(url.as_str()) {
            Ok(uri) => Box::new(warp::redirect::redirect(uri).into_response()),
            Err(err) => Box::new(warp::reply::with_status(
                format!("failed parse long url: {}", err),
                StatusCode::NOT_FOUND,
            )),
        },
        Err(YdbError::NoRows) => Box::new(warp::reply::with_status(
            "short url not found",
            StatusCode::NOT_FOUND,
        )),
        Err(err) => Box::new(warp::reply::with_status(
            format!("error while check short url: {}", err),
            StatusCode::INTERNAL_SERVER_ERROR,
        )),
    };
    return Ok(reply);
}

pub async fn run() -> Result<(), warp::Error> {
    // GET /[?l=][?url=]
    let index = warp::get().and_then(index);

    let url = warp::get().and(warp::query()).and_then(get_url);

    let redirect_page = warp::get().and(warp::query()).and_then(redirect);

    warp::serve(url.or(redirect_page).or(index))
        .run(([127, 0, 0, 1], 8000))
        .await;
    return Ok(());
}

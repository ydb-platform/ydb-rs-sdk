use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::NaiveDate;
use uuid::Uuid;
use ydb::Value;

use super::series_ops::{episode_row, season_row, series_row};

pub struct SampleData {
    pub series_example: Value,
    pub series: Vec<Value>,
    pub seasons_example: Value,
    pub seasons: Vec<Value>,
    pub episodes_example: Value,
    pub episodes: Vec<Value>,
}

pub fn sample_data() -> SampleData {
    let mut series = Vec::new();
    let mut seasons = Vec::new();
    let mut episodes = Vec::new();

    let it_crowd_id = Uuid::new_v4().as_bytes().to_vec();
    append_it_crowd(&it_crowd_id, &mut series, &mut seasons, &mut episodes);

    let silicon_valley_id = Uuid::new_v4().as_bytes().to_vec();
    append_silicon_valley(&silicon_valley_id, &mut series, &mut seasons, &mut episodes);

    let series_example = series_row(
        &it_crowd_id,
        date("2006-02-03"),
        "IT Crowd",
        "The IT Crowd is a British sitcom produced by Channel 4.",
        None,
    );
    let seasons_example = season_row(
        &it_crowd_id,
        Uuid::new_v4().as_bytes(),
        "Season 1",
        date("2006-02-03"),
        date("2006-03-03"),
    );
    let episodes_example = episode_row(
        &it_crowd_id,
        Uuid::new_v4().as_bytes(),
        Uuid::new_v4().as_bytes(),
        "Pilot",
        date("2006-02-03"),
    );

    SampleData {
        series_example,
        series,
        seasons_example,
        seasons,
        episodes_example,
        episodes,
    }
}

fn append_it_crowd(
    series_id: &[u8],
    series: &mut Vec<Value>,
    seasons: &mut Vec<Value>,
    episodes: &mut Vec<Value>,
) {
    series.push(series_row(
        series_id,
        date("2006-02-03"),
        "IT Crowd",
        "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by \
Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
        None,
    ));

    add_season(
        series_id,
        seasons,
        episodes,
        "Season 1",
        "2006-02-03",
        "2006-03-03",
        &[
            ("Yesterday's Jam", "2006-02-03"),
            ("Calamity Jen", "2006-02-03"),
            ("Fifty-Fifty", "2006-02-10"),
        ],
    );
    add_season(
        series_id,
        seasons,
        episodes,
        "Season 2",
        "2007-08-24",
        "2007-09-28",
        &[
            ("The Work Outing", "2006-08-24"),
            ("Return of the Golden Child", "2007-08-31"),
        ],
    );
}

fn append_silicon_valley(
    series_id: &[u8],
    series: &mut Vec<Value>,
    seasons: &mut Vec<Value>,
    episodes: &mut Vec<Value>,
) {
    series.push(series_row(
        series_id,
        date("2014-04-06"),
        "Silicon Valley",
        "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and \
Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
        Some("Some comment here"),
    ));

    add_season(
        series_id,
        seasons,
        episodes,
        "Season 1",
        "2014-04-06",
        "2014-06-01",
        &[
            ("Minimum Viable Product", "2014-04-06"),
            ("The Cap Table", "2014-04-13"),
            ("Proof of Concept", "2014-05-18"),
        ],
    );
    add_season(
        series_id,
        seasons,
        episodes,
        "Season 2",
        "2015-04-12",
        "2015-06-14",
        &[
            ("Sand Hill Shuffle", "2015-04-12"),
            ("Two Days of the Condor", "2015-06-14"),
        ],
    );
}

fn add_season(
    series_id: &[u8],
    seasons: &mut Vec<Value>,
    episodes: &mut Vec<Value>,
    title: &str,
    first: &str,
    last: &str,
    episode_defs: &[(&str, &str)],
) {
    let season_id = Uuid::new_v4().as_bytes().to_vec();
    seasons.push(season_row(
        series_id,
        &season_id,
        title,
        date(first),
        date(last),
    ));
    for (episode_title, aired) in episode_defs {
        episodes.push(episode_row(
            series_id,
            &season_id,
            Uuid::new_v4().as_bytes(),
            episode_title,
            date(aired),
        ));
    }
}

fn date(ymd: &str) -> SystemTime {
    let naive = NaiveDate::parse_from_str(ymd, "%Y-%m-%d").expect("valid date");
    let days = naive
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"))
        .num_days();
    UNIX_EPOCH + Duration::from_secs(days as u64 * 86_400)
}

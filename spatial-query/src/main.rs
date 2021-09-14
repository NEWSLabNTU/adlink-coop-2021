mod common;

use crate::common::*;
use tokio::sync::RwLock;

const MAX_DISTANCE: f64 = 50.0;

type SpatialTree = RTree<GeomWithData<[f64; 2], TreeEntry>>;

struct TreeEntry {
    pub key: String,
    pub x: f64,
    pub y: f64,
}

#[derive(Serialize)]
struct QueryResponse {
    pub items: Vec<QueriedItem>,
}

#[derive(Serialize)]
struct QueriedItem {
    pub key: String,
    pub x: f64,
    pub y: f64,
    pub distance: f64,
}

#[launch]
fn rocket() -> _ {
    let init_state = GlobalState {
        index_tree: RTree::new(),
        key_set: HashSet::new(),
    };

    rocket::build()
        .manage(Arc::new(RwLock::new(init_state)))
        .mount("/", routes![index, register, query])
}

#[get("/")]
async fn index() -> &'static str {
    "To register a key with a position,
/register?key=<key>&x=<x>&y=<y>

To query keys near a position,
/query?x=<x>&y=<y>
"
}

#[get("/register?<key>&<x>&<y>")]
async fn register(
    key: String,
    x: f64,
    y: f64,
    state: &State<Arc<RwLock<GlobalState>>>,
) -> Result<&str, Custom<&str>> {
    if key.is_empty() {
        return Err(Custom(
            Status::BadRequest,
            "the key string must not be empty",
        ));
    }

    let mut state = state.write().await;

    if state.key_set.contains(&key) {
        return Err(Custom(Status::Conflict, "the key is already registered"));
    }

    state.key_set.insert(key.clone());
    state
        .index_tree
        .insert(GeomWithData::new([x, y], TreeEntry { key, x, y }));

    Ok("Success")
}

#[get("/query?<x>&<y>")]
async fn query(x: f64, y: f64, state: &State<Arc<RwLock<GlobalState>>>) -> Json<QueryResponse> {
    let state = state.read().await;
    let items: Vec<_> = state
        .index_tree
        .locate_within_distance([x, y], MAX_DISTANCE)
        .map(|entry| {
            let data = &entry.data;
            let distance = ((x - data.x).powi(2) + (y - data.y).powi(2)).sqrt();

            QueriedItem {
                key: data.key.clone(),
                x: data.x,
                y: data.y,
                distance,
            }
        })
        .collect();

    Json(QueryResponse { items })
}

struct GlobalState {
    pub index_tree: SpatialTree,
    pub key_set: HashSet<String>,
}

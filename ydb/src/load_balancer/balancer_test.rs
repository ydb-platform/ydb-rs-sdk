use super::*;
use super::{
    nearest_dc_balancer::{BalancerConfig, FallbackStrategy, NearestDCBalancer},
    random_balancer::RandomLoadBalancer,
    LoadBalancer, MockLoadBalancer, SharedLoadBalancer,
};
use crate::discovery::NodeInfo;
use crate::grpc_wrapper::raw_services::Service::Table;
use crate::waiter::WaiterImpl;
use crate::YdbResult;
use http::Uri;
use itertools::Itertools;
use mockall::predicate;
use nearest_dc_balancer::{BalancerState, NODES_PER_DC, PING_TIMEOUT_SECS};
use ntest::assert_true;
use num::One;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::trace;

#[test]
fn shared_load_balancer() -> YdbResult<()> {
    let endpoint_counter = Arc::new(AtomicUsize::new(0));
    let test_uri = Uri::from_str("http://test.com")?;

    let mut lb_mock = MockLoadBalancer::new();
    let endpoint_counter_mock = endpoint_counter.clone();
    let test_uri_mock = test_uri.clone();

    lb_mock.expect_endpoint().returning(move |_service| {
        endpoint_counter_mock.fetch_add(1, Relaxed);
        Ok(test_uri_mock.clone())
    });

    let s1 = SharedLoadBalancer::new_with_balancer(Box::new(lb_mock));

    #[allow(clippy::redundant_clone)]
    let s2 = s1.clone();

    assert_eq!(test_uri, s1.endpoint(Table)?);
    assert_eq!(test_uri, s2.endpoint(Table)?);
    assert_eq!(endpoint_counter.load(Relaxed), 2);
    Ok(())
}

#[tokio::test]
async fn update_load_balancer_test() -> YdbResult<()> {
    let original_discovery_state = Arc::new(DiscoveryState::default());
    let (sender, receiver) = tokio::sync::watch::channel(original_discovery_state.clone());

    let new_discovery_state = Arc::new(DiscoveryState::default().with_node_info(
        Table,
        NodeInfo::new(Uri::from_str("http://test.com").unwrap(), String::new()),
    ));

    let (first_update_sender, first_update_receiver) = tokio::sync::oneshot::channel();
    let (second_update_sender, second_update_receiver) = tokio::sync::oneshot::channel();
    let (updater_finished_sender, updater_finished_receiver) =
        tokio::sync::oneshot::channel::<()>();

    let mut first_update_sender = Some(first_update_sender);
    let mut second_update_sender = Some(second_update_sender);
    let mut lb_mock = MockLoadBalancer::new();
    lb_mock
        .expect_set_discovery_state()
        .with(predicate::eq(original_discovery_state.clone()))
        .times(1)
        .returning(move |_| {
            trace!("first set");
            first_update_sender.take().unwrap().send(()).unwrap();
            Ok(())
        });

    lb_mock
        .expect_set_discovery_state()
        .with(predicate::eq(new_discovery_state.clone()))
        .times(1)
        .returning(move |_| {
            trace!("second set");
            second_update_sender.take().unwrap().send(()).unwrap();
            Ok(())
        });

    let shared_lb = SharedLoadBalancer::new_with_balancer(Box::new(lb_mock));

    tokio::spawn(async move {
        trace!("updater start");
        update_load_balancer(shared_lb, receiver).await;
        trace!("updater finished");
        updater_finished_sender.send(()).unwrap();
    });

    tokio::spawn(async move {
        first_update_receiver.await.unwrap();
        sender.send(new_discovery_state).unwrap();
        second_update_receiver.await.unwrap();
        drop(sender);
    });

    tokio::select! {
        _ = updater_finished_receiver =>{}
        _ = tokio::time::sleep(Duration::from_secs(10)) => {
            panic!("test failed");
        }
    }
    // updater_finished_receiver.await.unwrap();
    Ok(())
}

#[test]
fn random_load_balancer() -> YdbResult<()> {
    let one = Uri::from_str("http://one:213")?;
    let two = Uri::from_str("http://two:213")?;
    let load_balancer = RandomLoadBalancer {
        discovery_state: Arc::new(
            DiscoveryState::default()
                .with_node_info(Table, NodeInfo::new(one.clone(), String::new()))
                .with_node_info(Table, NodeInfo::new(two.clone(), String::new())),
        ),
        waiter: Arc::new(WaiterImpl::new()),
    };

    let mut map = HashMap::new();
    map.insert(one.to_string(), 0);
    map.insert(two.to_string(), 0);

    for _ in 0..100 {
        let u = load_balancer.endpoint(Table)?;
        let val = *map.get_mut(u.to_string().as_str()).unwrap();
        map.insert(u.to_string(), val + 1);
    }

    assert_eq!(map.len(), 2);
    assert!(*map.get(one.to_string().as_str()).unwrap() > 30);
    assert!(*map.get(two.to_string().as_str()).unwrap() > 30);
    Ok(())
}

#[test]
fn split_by_location() -> YdbResult<()> {
    let (one, two, three, four, five) = (
        NodeInfo::new(Uri::from_str("http://one:213")?, "A".to_string()),
        NodeInfo::new(Uri::from_str("http://two:213")?, "A".to_string()),
        NodeInfo::new(Uri::from_str("http://three:213")?, "B".to_string()),
        NodeInfo::new(Uri::from_str("http://four:213")?, "B".to_string()),
        NodeInfo::new(Uri::from_str("http://five:213")?, "C".to_string()),
    );

    let nodes = vec![
        one.clone(),
        two.clone(),
        three.clone(),
        four.clone(),
        five.clone(),
    ];

    let splitted = NearestDCBalancer::split_endpoints_by_location(&nodes);
    assert_eq!(
        splitted,
        HashMap::from([
            ("A".to_string(), vec![&one, &two]),
            ("B".to_string(), vec![&three, &four]),
            ("C".to_string(), vec![&five]),
        ])
    );
    Ok(())
}

#[test]
fn choose_random_endpoints() -> YdbResult<()> {
    let (one, two, three, four, five, six, seven, eight, nine) = (
        NodeInfo::new(Uri::from_str("http://one:213")?, "C".to_string()),
        NodeInfo::new(Uri::from_str("http://two:213")?, "C".to_string()),
        NodeInfo::new(Uri::from_str("http://three:213")?, "C".to_string()),
        NodeInfo::new(Uri::from_str("http://four:213")?, "C".to_string()),
        NodeInfo::new(Uri::from_str("http://five:213")?, "C".to_string()),
        NodeInfo::new(Uri::from_str("http://six:213")?, "C".to_string()),
        NodeInfo::new(Uri::from_str("http://seven:213")?, "C".to_string()),
        NodeInfo::new(Uri::from_str("http://eight:213")?, "C".to_string()),
        NodeInfo::new(Uri::from_str("http://nine:213")?, "C".to_string()),
    );

    let mut nodes_big = vec![
        &one, &two, &three, &four, &five, &six, &seven, &eight, &nine,
    ];

    let mut nodes_small = vec![&one, &two, &three];

    let random_subset_big = NearestDCBalancer::get_random_endpoints(&mut nodes_big);
    let random_subset_small = NearestDCBalancer::get_random_endpoints(&mut nodes_small);

    assert_eq!(random_subset_big.len(), NODES_PER_DC);
    assert_eq!(random_subset_small.len(), 3);

    Ok(())
}

#[test]
fn extract_addrs_and_map_them() -> YdbResult<()> {
    let one = NodeInfo::new(Uri::from_str("http://localhost:123")?, "C".to_string());
    let two = NodeInfo::new(Uri::from_str("http://localhost:321")?, "C".to_string());
    let nodes = vec![&one, &two];
    let map = NearestDCBalancer::addr_to_node(&nodes);

    assert_eq!(map.keys().len(), 4); // ipv4 + ipv6 on each
    assert_true!(map.keys().contains(&"127.0.0.1:123".to_string()));
    assert_true!(map.keys().contains(&"[::1]:123".to_string()));
    assert!(map["127.0.0.1:123"].eq(&one));
    assert!(map["127.0.0.1:123"].eq(map["[::1]:123"]));

    Ok(())
}

#[tokio::test]
async fn detect_fastest_addr_just_some() -> YdbResult<()> {
    let l1 = TcpListener::bind("127.0.0.1:0").await?;
    let l2 = TcpListener::bind("127.0.0.1:0").await?;
    let l3 = TcpListener::bind("127.0.0.1:0").await?;

    let l1_addr = l1.local_addr()?;
    let l2_addr = l2.local_addr()?;
    let l3_addr = l3.local_addr()?;

    println!("Listener №1 on: {}", l1_addr);
    println!("Listener №2 on: {}", l2_addr);
    println!("Listener №3 on: {}", l3_addr);

    let nodes = [
        l1_addr.to_string(),
        l2_addr.to_string(),
        l3_addr.to_string(),
    ];

    for _ in 0..100 {
        let addr = NearestDCBalancer::find_fastest_address(
            nodes.iter().collect_vec(),
            Duration::from_secs(PING_TIMEOUT_SECS),
        )
        .await?;
        assert!(nodes.contains(&addr))
    }

    Ok(())
}

#[tokio::test]
async fn detect_fastest_addr_with_fault() -> YdbResult<()> {
    let l1 = TcpListener::bind("127.0.0.1:0").await?;
    let l2 = TcpListener::bind("127.0.0.1:0").await?;
    let l3 = TcpListener::bind("127.0.0.1:0").await?;

    let l1_addr = l1.local_addr()?;
    let l2_addr = l2.local_addr()?;
    let l3_addr = l3.local_addr()?;

    println!("Listener №1 on: {}", l1_addr);
    println!("Listener №2 on: {}", l2_addr);
    println!("Listener №3 on: {}", l3_addr);

    let nodes = [
        l1_addr.to_string(),
        l2_addr.to_string(),
        l3_addr.to_string(),
    ];

    drop(l1);

    for _ in 0..100 {
        let addr = NearestDCBalancer::find_fastest_address(
            nodes.iter().collect_vec(),
            Duration::from_secs(PING_TIMEOUT_SECS),
        )
        .await?;
        assert!(nodes.contains(&addr) && addr != l1_addr.to_string())
    }

    Ok(())
}

#[tokio::test]
async fn detect_fastest_addr_one_alive() -> YdbResult<()> {
    let l1 = TcpListener::bind("127.0.0.1:0").await?;
    let l2 = TcpListener::bind("127.0.0.1:0").await?;
    let l3 = TcpListener::bind("127.0.0.1:0").await?;

    let l1_addr = l1.local_addr()?;
    let l2_addr = l2.local_addr()?;
    let l3_addr = l3.local_addr()?;

    println!("Listener №1 on: {}", l1_addr);
    println!("Listener №2 on: {}", l2_addr);
    println!("Listener №3 on: {}", l3_addr);

    let nodes = [
        l1_addr.to_string(),
        l2_addr.to_string(),
        l3_addr.to_string(),
    ];

    drop(l1);
    drop(l2);

    for _ in 0..100 {
        let addr = NearestDCBalancer::find_fastest_address(
            nodes.iter().collect_vec(),
            Duration::from_secs(PING_TIMEOUT_SECS),
        )
        .await?;
        assert!(addr == l3_addr.to_string())
    }

    Ok(())
}

#[tokio::test]
async fn detect_fastest_addr_timeout() -> YdbResult<()> {
    let l1 = TcpListener::bind("127.0.0.1:0").await?;
    let l2 = TcpListener::bind("127.0.0.1:0").await?;
    let l3 = TcpListener::bind("127.0.0.1:0").await?;

    let l1_addr = l1.local_addr()?;
    let l2_addr = l2.local_addr()?;
    let l3_addr = l3.local_addr()?;

    println!("Listener №1 on: {}", l1_addr);
    println!("Listener №2 on: {}", l2_addr);
    println!("Listener №3 on: {}", l3_addr);

    let nodes = [
        l1_addr.to_string(),
        l2_addr.to_string(),
        l3_addr.to_string(),
    ];

    drop(l1);
    drop(l2);
    drop(l3);

    let result =
        NearestDCBalancer::find_fastest_address(nodes.iter().collect_vec(), Duration::from_secs(3))
            .await;
    match result {
        Ok(_) => unreachable!(),
        Err(err) => {
            assert_eq!(
                err.to_string(),
                "Custom(\"timeout while detecting fastest address\")"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn no_addr_timeout() -> YdbResult<()> {
    let result = NearestDCBalancer::find_fastest_address(Vec::new(), Duration::from_secs(3)).await;
    match result {
        Ok(_) => unreachable!(),
        Err(err) => {
            assert_eq!(
                err.to_string(),
                "Custom(\"timeout while detecting fastest address\")"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn adjusting_dc() -> YdbResult<()> {
    let l1 = TcpListener::bind("127.0.0.1:0").await?;
    let l2 = TcpListener::bind("127.0.0.1:0").await?;
    let l3 = TcpListener::bind("127.0.0.1:0").await?;

    let l1_addr = l1.local_addr()?;
    let l2_addr = l2.local_addr()?;
    let l3_addr = l3.local_addr()?;

    println!("Listener №1 on: {}", l1_addr);
    println!("Listener №2 on: {}", l2_addr);
    println!("Listener №3 on: {}", l3_addr);

    let discovery_state = Arc::new(DiscoveryState::default());
    let balancer_state = Arc::new(RwLock::new(BalancerState::default()));
    let balancer_state_updater = balancer_state.clone();
    let (state_sender, state_reciever) = watch::channel(discovery_state.clone());

    let ping_token = CancellationToken::new();
    let ping_token_clone = ping_token.clone();

    let waiter = Arc::new(WaiterImpl::new());
    let waiter_clone = waiter.clone();

    let updater = tokio::spawn(async move {
        NearestDCBalancer::adjust_local_dc(
            balancer_state_updater,
            state_reciever,
            ping_token_clone,
            waiter_clone,
        )
        .await
    });

    let updated_state = Arc::new(
        DiscoveryState::default()
            .with_node_info(
                Table,
                NodeInfo::new(
                    Uri::from_str(l1_addr.to_string().as_str()).unwrap(),
                    "A".to_string(),
                ),
            )
            .with_node_info(
                Table,
                NodeInfo::new(
                    Uri::from_str(l2_addr.to_string().as_str()).unwrap(),
                    "B".to_string(),
                ),
            )
            .with_node_info(
                Table,
                NodeInfo::new(
                    Uri::from_str(l2_addr.to_string().as_str()).unwrap(),
                    "C".to_string(),
                ),
            ),
    );
    assert!(balancer_state
        .read()
        .unwrap()
        .borrow()
        .preferred_endpoints
        .is_empty());
    let _ = state_sender.send(updated_state);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_true!(timeout(Duration::from_secs(3), waiter.wait()).await.is_ok()); // should not wait
    assert!(
        balancer_state
            .read()
            .unwrap()
            .borrow()
            .preferred_endpoints
            .len()
            .is_one() // only one endpoint in each dc
    );
    let updated_state_next = Arc::new(
        DiscoveryState::default()
            .with_node_info(
                Table,
                NodeInfo::new(
                    Uri::from_str(l1_addr.to_string().as_str()).unwrap(),
                    "A".to_string(),
                ),
            )
            .with_node_info(
                Table,
                NodeInfo::new(
                    Uri::from_str(l2_addr.to_string().as_str()).unwrap(),
                    "A".to_string(),
                ),
            ),
    );
    let _ = state_sender.send(updated_state_next);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_true!(timeout(Duration::from_secs(3), waiter.wait()).await.is_ok()); // should not wait
    assert!(
        balancer_state
            .read()
            .unwrap()
            .borrow()
            .preferred_endpoints
            .len()
            == 2 // both endpoints in same dc
    );
    ping_token.cancel(); // reciever stops wait for state change
    let _ = tokio::join!(updater); // should join
    Ok(())
}

#[tokio::test]
async fn nearest_dc_balancer_integration_with_error_fallback() -> YdbResult<()> {
    let balancer = NearestDCBalancer::new(BalancerConfig {
        fallback_strategy: FallbackStrategy::Error,
    })
    .unwrap();

    let sh = SharedLoadBalancer::new_with_balancer(Box::new(balancer));

    match sh.endpoint(Table) {
        Ok(_) => unreachable!(),
        Err(err) => assert_eq!(
            err.to_string(),
            "Custom(\"no available endpoints for service:table_service\")".to_string()
        ),
    }
    Ok(())
}

#[tokio::test]
async fn nearest_dc_balancer_integration_with_other_fallback_error() -> YdbResult<()> {
    let balancer = NearestDCBalancer::new(BalancerConfig::default()).unwrap();

    let sh = SharedLoadBalancer::new_with_balancer(Box::new(balancer));

    match sh.endpoint(Table) {
        Ok(_) => unreachable!(),
        Err(err) => assert_eq!(
            err.to_string(),
            "Custom(\"empty endpoint list for service: table_service\")".to_string()
        ),
    }
    Ok(())
}

#[tokio::test]
async fn nearest_dc_balancer_integration() -> YdbResult<()> {
    let l1 = TcpListener::bind("127.0.0.1:0").await?;
    let l2 = TcpListener::bind("127.0.0.1:0").await?;

    let l1_addr = l1.local_addr()?;
    let l2_addr = l2.local_addr()?;

    println!("Listener №1 on: {}", l1_addr);
    println!("Listener №2 on: {}", l2_addr);

    let balancer = NearestDCBalancer::new(BalancerConfig {
        fallback_strategy: FallbackStrategy::Error,
    })
    .unwrap();

    let sh = SharedLoadBalancer::new_with_balancer(Box::new(balancer));
    let self_updater = sh.clone();
    let (state_sender, state_reciever) =
        watch::channel::<Arc<DiscoveryState>>(Arc::new(DiscoveryState::default()));

    tokio::spawn(async move { update_load_balancer(self_updater, state_reciever).await });

    match sh.endpoint(Table) {
        Ok(_) => unreachable!(),
        Err(err) => assert_eq!(
            err.to_string(),
            "Custom(\"no available endpoints for service:table_service\")".to_string()
        ),
    }
    let updated_state = Arc::new(
        DiscoveryState::default()
            .with_node_info(
                Table,
                NodeInfo::new(
                    Uri::from_str(l1_addr.to_string().as_str()).unwrap(),
                    "A".to_string(),
                ),
            )
            .with_node_info(
                Table,
                NodeInfo::new(
                    Uri::from_str(l2_addr.to_string().as_str()).unwrap(),
                    "A".to_string(),
                ),
            ),
    );

    let _ = state_sender.send(updated_state);

    sh.wait().await?;

    match sh.endpoint(Table) {
        Ok(uri) => {
            let addr = uri.host().unwrap();
            assert!(addr == "127.0.0.1" || addr == "[::1]")
        }
        Err(err) => unreachable!("{}", err.to_string()),
    }
    Ok(())
}

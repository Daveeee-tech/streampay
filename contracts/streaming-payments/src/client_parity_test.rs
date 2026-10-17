//! Shared client-parity golden scenarios, executed through the Soroban test client.
#![cfg(test)]

use std::string::ToString;

use soroban_sdk::{
    testutils::{Address as _, Events as _, Ledger as _},
    token::{Client as TokenClient, StellarAssetClient},
    Address, Env, Event as _,
};

use crate::{
    test_client::StreamingPaymentsClient as Client, StreamCreated, StreamingPayments, Withdrawn,
};

const FIXTURES: &str = include_str!("../../../tests/client-parity/fixtures.json");
const INITIAL_BALANCE: i128 = 1_000;

#[test]
fn shared_client_parity_fixtures_match_event_xdr_and_balances() {
    let mut fixtures: serde_json::Value =
        serde_json::from_str(FIXTURES).expect("valid parity fixtures");
    assert_eq!(fixtures["schemaVersion"], 1);
    let cases = fixtures["cases"].as_array_mut().expect("fixture cases");
    assert!(!cases.is_empty(), "parity fixture corpus must not be empty");
    let bless = matches!(std::env::var("STREAMPAY_PARITY_BLESS").as_deref(), Ok("1"));

    for case in cases.iter_mut() {
        let mut env = Env::default();
        env.set_config(soroban_sdk::testutils::EnvTestConfig {
            capture_snapshot_at_drop: false,
        });
        env.mock_all_auths();
        let token = env
            .register_stellar_asset_contract_v2(Address::generate(&env))
            .address();
        let sender = Address::generate(&env);
        let recipient = Address::generate(&env);
        let governor = Address::generate(&env);
        let contract_id = env.register(StreamingPayments, (governor,));
        let client = Client::new(&env, &contract_id);
        let asset = StellarAssetClient::new(&env, &token);
        asset.mint(&sender, &INITIAL_BALANCE);
        asset.approve(&sender, &contract_id, &INITIAL_BALANCE, &1_000_000u32);

        let deposit = case["deposit"].as_str().unwrap().parse::<i128>().unwrap();
        let start_time = case["startTime"].as_str().unwrap().parse::<u64>().unwrap();
        let end_time = case["endTime"].as_str().unwrap().parse::<u64>().unwrap();
        let withdraw_time = case["withdrawTime"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let withdraw_amount = case["withdrawAmount"]
            .as_str()
            .unwrap()
            .parse::<i128>()
            .unwrap();
        let cancelable = case["cancelable"].as_bool().unwrap();

        env.ledger()
            .with_mut(|ledger| ledger.timestamp = start_time);
        let stream_id = client.mock_all_auths().create_stream(
            &sender,
            &recipient,
            &token,
            &deposit,
            &start_time,
            &end_time,
            &cancelable,
        );
        env.ledger()
            .with_mut(|ledger| ledger.timestamp = withdraw_time);
        client
            .mock_all_auths()
            .withdraw(&stream_id, &withdraw_amount);

        let token_client = TokenClient::new(&env, &token);
        if bless {
            case["events"][0]["payload"] = serde_json::json!({
                "stream_id": stream_id.to_string(),
                "deposit": deposit.to_string(),
                "start_time": start_time.to_string(),
                "end_time": end_time.to_string(),
                "cancelable": cancelable
            });
            case["events"][1]["payload"] = serde_json::json!({
                "stream_id": stream_id.to_string(),
                "amount": withdraw_amount.to_string(),
                "withdrawn_total": withdraw_amount.to_string()
            });
            case["balances"] = serde_json::json!({
                "sender": token_client.balance(&sender).to_string(),
                "recipient": token_client.balance(&recipient).to_string(),
                "contract": token_client.balance(&contract_id).to_string()
            });
        }

        let expected_created = &case["events"][0]["payload"];
        let expected_withdrawn = &case["events"][1]["payload"];
        assert_eq!(case["events"][0]["name"], "StreamCreated");
        assert_eq!(case["events"][1]["name"], "Withdrawn");
        let expected_events = [
            StreamCreated {
                stream_id: expected_created["stream_id"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                sender: sender.clone(),
                recipient: recipient.clone(),
                token: token.clone(),
                deposit: expected_created["deposit"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                start_time: expected_created["start_time"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                end_time: expected_created["end_time"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                cancelable: expected_created["cancelable"].as_bool().unwrap(),
            }
            .to_xdr(&env, &contract_id),
            Withdrawn {
                stream_id: expected_withdrawn["stream_id"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                recipient: recipient.clone(),
                amount: expected_withdrawn["amount"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                withdrawn_total: expected_withdrawn["withdrawn_total"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
            }
            .to_xdr(&env, &contract_id),
        ];
        let actual_events = env.events().all().filter_by_contract(&contract_id);
        assert_eq!(
            actual_events, expected_events,
            "event XDR diverged for fixture {}",
            case["id"]
        );
        let balances = &case["balances"];
        assert_eq!(
            token_client.balance(&sender).to_string(),
            balances["sender"].as_str().unwrap()
        );
        assert_eq!(
            token_client.balance(&recipient).to_string(),
            balances["recipient"].as_str().unwrap()
        );
        assert_eq!(
            token_client.balance(&contract_id).to_string(),
            balances["contract"].as_str().unwrap()
        );
    }

    if bless {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/client-parity/fixtures.json");
        std::fs::write(
            path,
            serde_json::to_string_pretty(&fixtures).unwrap() + "\n",
        )
        .expect("write blessed parity fixture");
    }
}

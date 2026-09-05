#[tokio::test]
async fn query_http_validation_and_storage_administration_are_explicit() {
    assert!(format!("{:?}", CoverageServer::new(config()).unwrap()).contains("CoverageServer"));
    let mut invalid = config();
    invalid.http_max_body_bytes = 1;
    assert!(CoverageServer::new(invalid).is_err());
    let mut invalid = config();
    invalid.host = "example.com".into();
    assert!(CoverageServer::new(invalid).unwrap().run().await.is_err());
    assert!(error_string(AppError::Validation("test".into())).contains("test"));
    let query = query_params(&"/?n=3&n=4&bad=no".parse().unwrap());
    assert_eq!(query_value(&query, "n"), Some("3"));
    assert_eq!(query_usize(&query, "n", 1).unwrap(), 3);
    assert!(query_usize(&query, "bad", 1).is_err());
    assert!(required_body(&json!({}), "report_path").is_err());
    assert!(optional_body_string(&json!({"report_path":1}), "report_path").is_err());
    let patch=project_patch(&json!({"compaction":{"compaction_enabled":false,"compaction_after_days":7,"compaction_interval_seconds":60,"compaction_batch_size":2}})).unwrap();
    assert_eq!(patch.compaction_after_days, Some(7));
    for body in [
        json!([]),
        json!({"compaction_enabled":3}),
        json!({"compaction_after_days":"x"}),
        json!({"compaction_after_days":u64::MAX}),
        json!({"compaction_interval_seconds":-1}),
        json!({"compaction_batch_size":0}),
    ] {
        if body["compaction_batch_size"] == 0 {
            continue;
        }
        assert!(project_patch(&body).is_err());
    }
    assert!(project_patch(&json!({})).is_ok());
    let directory = tempfile::tempdir().unwrap();
    let repo = directory.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    let common = directory.path().join("common.duckdb");
    let local = default_db_path(&repo);
    let legacy = common
        .parent()
        .unwrap()
        .join("projects")
        .join(format!("{}.duckdb", key_hash(repo.to_str().unwrap())));
    std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
    std::fs::write(&legacy, []).unwrap();
    assert_eq!(
        project_database_path(&common, repo.to_str().unwrap()),
        legacy
    );
    std::fs::create_dir_all(local.parent().unwrap()).unwrap();
    std::fs::write(&local, []).unwrap();
    assert_eq!(
        project_database_path(&common, repo.to_str().unwrap()),
        local
    );
    std::fs::remove_file(&local).unwrap();
    std::fs::remove_file(&legacy).unwrap();
    std::fs::write(repo.join("r.lcov"), "TN:\nSF:a.rs\nDA:1,0\nend_of_record\n").unwrap();
    let mut cfg = config();
    cfg.common_db_path = common;
    let server = CoverageServer::new(cfg).unwrap();
    let service = server
        .service_for_repository_path(repo.to_str().unwrap())
        .unwrap();
    assert!(
        registry_project_summary("selected", Ok(service))
            .get("repo_key")
            .is_some()
    );
    let mut body = Vec::new();
    append_body_data(&mut body, None, 1).unwrap();
    assert!(body.is_empty());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(server.clone().serve_listener(listener));
    async fn request(
        address: std::net::SocketAddr,
        method: &str,
        path: &str,
        body: &str,
        repo: Option<&str>,
    ) -> String {
        let header = repo
            .map(|repo| format!("{REPOSITORY_HEADER}: {repo}\r\n"))
            .unwrap_or_default();
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket.write_all(format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{header}Content-Length: {}\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.unwrap();
        String::from_utf8(bytes).unwrap()
    }
    for (method, path, body, status, selected) in [
        ("GET", "/favicon.ico", "", "204", false),
        ("GET", "/mcp", "", "405", false),
        ("POST", "/mcp", "{}", "400", false),
        (
            "POST",
            "/mcp",
            "{\"method\":\"notifications/initialized\"}",
            "202",
            false,
        ),
        ("GET", "/api/snapshots", "", "400", false),
        ("GET", "/api/projects", "", "200", false),
        ("POST", "/api/ingest", "{}", "400", true),
        ("POST", "/api/ingest", "{\"report_path\":7}", "400", true),
        (
            "POST",
            "/api/ingest",
            "{\"report_path\":\"r.lcov\",\"format\":\"lcov\",\"suite\":\"unit\",\"branch\":\"main\",\"commit_sha\":\"abc\",\"base_ref\":\"base\"}",
            "200",
            true,
        ),
        (
            "PATCH",
            "/api/projects/project",
            "{\"compaction_enabled\":false,\"compaction_after_days\":1}",
            "200",
            true,
        ),
        ("POST", "/api/projects/project/compact", "{}", "200", true),
        (
            "GET",
            "/api/snapshots?suite=unit&branch=main&max_words=600",
            "",
            "200",
            true,
        ),
        ("GET", "/api/projects?max_words=bad", "", "400", true),
        ("GET", "/api/snapshots?cursor=bad", "", "400", true),
        (
            "POST",
            "/api/coverage/gaps",
            "{\"measurement\":\"r.lcov\",\"limit\":21}",
            "400",
            true,
        ),
        (
            "POST",
            "/mcp",
            "{\"method\":\"resources/read\",\"id\":1}",
            "200",
            true,
        ),
        ("GET", "/api/projects/unknown", "", "404", false),
    ] {
        let response = request(
            address,
            method,
            path,
            body,
            if selected { repo.to_str() } else { None },
        )
        .await;
        assert!(
            response.starts_with(&format!("HTTP/1.1 {status}")),
            "{method} {path}: {response}"
        );
    }
    let payload = format!(
        "{{\"report_path\":\"r.lcov\",\"repo_path\":{}}}",
        json!(repo)
    );
    assert!(
        request(address, "POST", "/api/ingest", &payload, repo.to_str())
            .await
            .contains("200 OK")
    );
    assert!(
        request(
            address,
            "GET",
            &format!("/api/snapshots?repo_path={}", repo.display()),
            "",
            None
        )
        .await
        .contains("200 OK")
    );
    let project_id = crate::stable_project_id(&crate::git::inspect_git(&repo).unwrap().repo_key);
    assert!(
        request(
            address,
            "PATCH",
            &format!("/api/projects/{project_id}"),
            "{}",
            None
        )
        .await
        .contains("200 OK")
    );
    assert!(
        request(
            address,
            "POST",
            &format!("/api/projects/{project_id}/compact"),
            "{}",
            None
        )
        .await
        .contains("200 OK")
    );
    for field in [
        "repo_path",
        "format",
        "suite",
        "branch",
        "commit_sha",
        "base_ref",
    ] {
        let mut body = json!({"report_path":"r.lcov"});
        body[field] = json!(7);
        assert!(
            request(
                address,
                "POST",
                "/api/ingest",
                &body.to_string(),
                repo.to_str()
            )
            .await
            .contains("400 Bad Request")
        );
    }
    for (method, path, body, selected) in [
        ("POST", "/api/ingest", "{", true),
        (
            "POST",
            "/api/ingest",
            "{\"report_path\":\"r.lcov\",\"format\":\"bad\"}",
            true,
        ),
        ("GET", "/api/projects?max_words=1", "", true),
        (
            "PATCH",
            "/api/projects/project",
            "{\"compaction_batch_size\":0}",
            true,
        ),
        (
            "PATCH",
            "/api/projects/project",
            "{\"compaction_batch_size\":\"bad\"}",
            true,
        ),
        (
            "POST",
            "/mcp",
            "{\"method\":\"tools/call\",\"id\":1}",
            false,
        ),
        ("GET", "/api/snapshots?repo_path=%00", "", false),
    ] {
        let response = request(
            address,
            method,
            path,
            body,
            if selected { repo.to_str() } else { None },
        )
        .await;
        assert!(
            response.contains("400 Bad Request"),
            "{path} {body}: {response}"
        );
    }
    let mismatch = json!({"report_path":"r.lcov","repo_path":directory.path()});
    assert!(
        request(
            address,
            "POST",
            "/api/ingest",
            &mismatch.to_string(),
            repo.to_str()
        )
        .await
        .contains("400 Bad Request")
    );
    let service = server
        .service_for_repository_path(repo.to_str().unwrap())
        .unwrap();
    service
        .store()
        .execute_sql_for_test("DROP TABLE snapshots")
        .unwrap();
    for (method, path) in [
        ("GET", "/api/snapshots"),
        ("POST", "/api/projects/project/compact"),
    ] {
        assert!(
            request(address, method, path, "{}", repo.to_str())
                .await
                .contains("500 Internal Server Error")
        );
    }
    task.abort();
    let _ = task.await;
    server.close_stores().unwrap();
}

#[tokio::test]
async fn runtime_faults_remain_explicit_after_route_removal() {
    assert!(!trusted_host(&HeaderMap::new()).unwrap());
    let mut headers = HeaderMap::new();
    headers.insert(HOST, HeaderValue::from_bytes(&[0xff]).unwrap());
    assert!(trusted_host(&headers).is_err());
    assert!(append_body_data(&mut vec![], Some(&Bytes::from_static(b"xx")), 1).is_err());
    assert!(
        combine_shutdown_results(Ok(()), Err(AppError::Runtime("close failed".into()))).is_err()
    );
    #[cfg(unix)]
    assert!(
        terminate_signal_future_with(|| Err(std::io::Error::other("registration failed"))).is_err()
    );
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.common_db_path = dir.path().join("common.duckdb");
    let server = CoverageServer::new(cfg.clone()).unwrap();
    std::fs::write(&cfg.common_db_path, "invalid database").unwrap();
    assert!(
        server
            .service_for_repository_path(dir.path().to_str().unwrap())
            .is_err()
    );
    std::fs::remove_file(&cfg.common_db_path).unwrap();
    let held = CoverageStore::open(default_db_path(dir.path()), cfg.clone()).unwrap();
    assert!(
        server
            .service_for_repository_path(dir.path().to_str().unwrap())
            .is_err()
    );
    held.close().unwrap();
    let service = server
        .service_for_repository_path(dir.path().to_str().unwrap())
        .unwrap();
    service
        .store()
        .execute_sql_for_test("DROP TABLE project_settings")
        .unwrap();
    assert!(
        server
            .service_for_repository_path(dir.path().to_str().unwrap())
            .is_err()
    );
    assert_eq!(
        registry_project_summary("repo", Ok(service))["status"],
        "unavailable"
    );
    server.close_stores().unwrap();
}

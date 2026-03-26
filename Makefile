show_bucket_size:
	@gcloud storage du -s gs://${TF_VAR_bucket_taxi}/raw | awk '{printf "%.2f MB\t%s\n", $$1/1024/1024, $$2}'
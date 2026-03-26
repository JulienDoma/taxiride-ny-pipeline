resource "google_service_account" "data_pipeline_sa" {
  account_id   = "${var.environment}-data-pipeline-sa"
  display_name = "Data Pipeline Service Account"
  description  = "Service account for data pipeline operations in ${var.environment} environment"
}

resource "google_storage_bucket_iam_member" "data_pipeline_bucket_admin" {
  bucket = google_storage_bucket.data_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.data_pipeline_sa.email}"
}
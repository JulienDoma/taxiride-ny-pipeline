resource "google_storage_bucket" "data_bucket" {
  name     = "${var.bucket_taxi}"
  location = "us-east1"

  force_destroy = true

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}
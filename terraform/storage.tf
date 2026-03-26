resource "google_storage_bucket" "data_bucket" {
  name     = "${var.project_id}-${var.environment}-terraform-data"
  location = "us-east1"

  force_destroy = true

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}
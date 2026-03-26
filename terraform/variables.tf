# Project level variables
variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "region" {
  type        = string
  default     = "europe-west1"
  description = "GCP region"
}

# Environment
variable "environment" {
  type        = string
  description = "Environment name"
}

# Storage
variable "bucket_taxi" {
  type        = string
  description = "GCP Bucket"
}

variable "credentials_file" {
  description = "Path to the credentials file"
  default     = "~/.google/credentials/dezc-credentials.json"
}

variable "project" {
  description = "Project"
  default     = "shdezc"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "shdezc_instacart_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket name"
  default     = "shdezc_instacart_bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

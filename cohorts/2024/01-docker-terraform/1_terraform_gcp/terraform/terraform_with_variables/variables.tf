variable "credentials" {
    description = "Project credentials"
    default = "../keys/my-creds.json"
}

variable "project" {
    description = "Project description"
    default = "terraform-demo-416705"
}

variable "location" {
    description = "Project locatoin"
    default = "US"
}
variable "region" {
    description = "Project region"
    default = "us-central1"
}

variable "bq_dataset_name" {
    description = "My bigQuery dataset name"
    default = "demo_dataset"
}

variable "gcs_bucket_name" {
    description = "My storage bucket name"
    default =  "terraform-demo-416705-bucket"
}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default =  "STANDARD"
}
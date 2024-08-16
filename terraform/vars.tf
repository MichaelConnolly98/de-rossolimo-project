variable "ingestion_bucket_prefix" {
  type    = string
  default = "de-rossolimo-ingestion"
}


variable "code_bucket_prefix" {
  type    = string
  default = "de-rossolimo-code"
}

variable "extract_lambda" {
  type = string
  default = "extract"
}

variable "emails" {
  type = list(string)
  sensitive = true
}

variable "processed_data_bucket_prefix" {
  type    = string
  default = "de-rossolimo-processed"
}
variable "ingestion_bucket_prefix" {
  type    = string
  default = "de-rossolimo-ingestion"
}


variable "code_bucket_prefix" {
  type    = string
  default = "de-rossolimo-code"
}

variable "process_bucket_prefix" {
  type = string
  default = "de-rossolimo-processed"
}

variable "extract_lambda" {
  type = string
  default = "extract"
}

variable "transform_lambda" {
  type = string
  default = "transform"
}

variable "load_lambda" {
  type = string
  default = "load"
}

variable "emails" {
  type = list(string)
}

variable "processed_data_bucket_prefix" {
  type    = string
  default = "de-rossolimo-processed"
}
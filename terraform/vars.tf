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


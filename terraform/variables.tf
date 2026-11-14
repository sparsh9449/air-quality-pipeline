variable "kafka_topic" {
  description = "Topic name for AQI measurements."
  type        = string
  default     = "aqi.measurements"
}

variable "redpanda_image" {
  description = "Redpanda container image."
  type        = string
  default     = "redpandadata/redpanda:v24.2.7"
}

# variables.tf

variable "aws_region" {
  description = "The AWS region to deploy resources"
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "The name of the S3 bucket for Alpha Vantage dataset storage"
  default     = "alpha-vantage-data-bucket"
}

variable "environment" {
  description = "Environment for tagging purposes"
  default     = "Development"
}

variable "ec2_ami" {
  description = "The AMI ID for the EC2 instance"
  default     = "ami-0e2c8caa4b6378d8c" 
}

variable "ec2_instance_type" {
  description = "The instance type for the EC2 instance"
  default     = "t2.micro"
}

variable "ec2_key_name" {
  description = "The name of the key pair for the EC2 instance"
  default     = "ML_KeyPair"  
}

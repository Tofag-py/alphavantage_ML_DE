provider "aws" {
  region = var.aws_region  # AWS region from variables
}

# S3 Bucket for Alpha Vantage Dataset Storage
resource "aws_s3_bucket" "alpha_vantage_data" {
  bucket = var.s3_bucket_name

  tags = {
    Name        = "Alpha_Vantage_Data_Bucket"
    Environment = var.environment
  }
}

# EC2 Instance for Machine Learning Processing
resource "aws_instance" "ml_ec2_instance" {
  ami           = var.ec2_ami
  instance_type = var.ec2_instance_type
  key_name      = var.ec2_key_name

  tags = {
    Name        = "Alpha_Vantage_ML_EC2"
    Environment = var.environment
  }
}

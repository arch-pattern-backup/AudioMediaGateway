import json
import os

class ConfigManager:
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = {}
        self.load_config()

    def load_config(self):
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    self.config = json.load(f)
                
                # S3 Defaults
                if "storage_type" not in self.config: self.config["storage_type"] = "local"
                if "s3_endpoint" not in self.config: self.config["s3_endpoint"] = ""
                if "s3_bucket" not in self.config: self.config["s3_bucket"] = ""
                if "s3_region" not in self.config: self.config["s3_region"] = ""
                if "s3_access_key" not in self.config: self.config["s3_access_key"] = ""
                if "s3_secret_key" not in self.config: self.config["s3_secret_key"] = ""
                if "s3_path_prefix" not in self.config: self.config["s3_path_prefix"] = ""
            except:
                self.config = {}
        else:
            self.config = {}

    def save_config(self):
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=4)
        except:
            pass

    def get(self, key, default=None):
        return self.config.get(key, default)

    def set(self, key, value):
        self.config[key] = value
        self.save_config()

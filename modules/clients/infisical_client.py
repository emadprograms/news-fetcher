from infisical_sdk import InfisicalSDKClient
import os
import toml
from dotenv import load_dotenv

class InfisicalManager:
    def __init__(self, project_id=None):
        """
        Initializes the Infisical Client using Universal Auth (Client ID & Secret).
        Priority: 
        1. Environment Vars (INFISICAL_CLIENT_ID, INFISICAL_CLIENT_SECRET)
        2. Local Secrets (secrets.toml -> [infiscal] section)
        """
        load_dotenv()
        self.client = None
        self.is_connected = False
        self.project_id = project_id 
        
        # 1. Try Streamlit Secrets First (for App compatibility)
        client_id = None
        client_secret = None
        try:
            import streamlit as st
            sec = st.secrets.get("infisical")
            if sec:
                client_id = sec.get("client_id")
                client_secret = sec.get("client_secret")
                if not self.project_id: self.project_id = sec.get("project_id")
                if client_id: print("🛡️ Using Streamlit Secrets.")
        except Exception:
            pass

        # 2. Try Env Vars if still missing
        if not client_id:
            client_id = os.getenv("INFISICAL_CLIENT_ID")
        if not client_secret:
            client_secret = os.getenv("INFISICAL_CLIENT_SECRET")
        if not self.project_id:
            self.project_id = os.getenv("INFISICAL_PROJECT_ID")
        
        # 2.5 Resolve Environment
        self.default_env = os.getenv("INFISICAL_ENV", "dev")
        
        # 3. Last Resort: Local secrets.toml (Direct file read for CLI)
        if not client_id or not client_secret or not self.project_id:
            try:
                data = toml.load(".streamlit/secrets.toml")
                sec = data.get("infisical")
                if sec:
                    if not client_id: client_id = sec.get("client_id")
                    if not client_secret: client_secret = sec.get("client_secret")
                    if not self.project_id: self.project_id = sec.get("project_id")
                    if not self.default_env and sec.get("environment"):
                        self.default_env = sec.get("environment")
            except Exception:
                pass

        if client_id and client_secret:
            try:
                self.client = InfisicalSDKClient(host="https://app.infisical.com")
                self.client.auth.universal_auth.login(
                    client_id=client_id,
                    client_secret=client_secret
                )
                self.is_connected = True
                print(f"✅ Infisical Client Connected (Env: {self.default_env}).")
            except Exception as e:
                print(f"❌ Infisical Connection Failed: {e}")
        else:
            print("⚠️ Infisical Credentials not found in ENV or secrets.toml.")

    def _extract_value(self, secret_obj):
        """ Helper to extract value from various Infisical SDK response versions. """
        if not secret_obj: return None
        
        # Strategy A: Check for nested 'secret' object (v3 SDK)
        if hasattr(secret_obj, 'secret'):
            nested = getattr(secret_obj, 'secret')
            for attr in ['secret_value', 'secretValue', 'value']:
                if hasattr(nested, attr):
                    return getattr(nested, attr)
        
        # Strategy B: Try direct attributes (v2 SDK or fallback)
        for attr in ['secret_value', 'secretValue', 'value']:
            if hasattr(secret_obj, attr):
                return getattr(secret_obj, attr)
        
        # Strategy C: Try dict access
        if isinstance(secret_obj, dict):
            return secret_obj.get('secret_value') or secret_obj.get('secretValue') or secret_obj.get('value')
            
        return None

    def _extract_key_name(self, secret_obj):
        """ Helper to extract key name from various Infisical SDK response versions. """
        if not secret_obj: return None
        
        # Strategy A: Check for nested 'secret' object (v3 SDK)
        if hasattr(secret_obj, 'secret'):
            nested = getattr(secret_obj, 'secret')
            for attr in ['secret_key', 'secretKey', 'key']:
                if hasattr(nested, attr):
                    return getattr(nested, attr)

        # Strategy B: Try direct attributes
        for attr in ['secret_key', 'secretKey', 'key']:
            if hasattr(secret_obj, attr):
                return getattr(secret_obj, attr)
        
        # Strategy C: Try dict access
        if isinstance(secret_obj, dict):
            return secret_obj.get('secret_key') or secret_obj.get('secretKey') or secret_obj.get('key')
            
        return None

    def get_secret(self, secret_name, environment=None, path="/"):
        """
        Retrieves a single secret value.
        """
        if not self.is_connected:
            return None
        
        env = environment or self.default_env
        
        try:
            response = self.client.secrets.get_secret_by_name(
                secret_name=secret_name,
                project_id=self.project_id, 
                environment_slug=env,
                secret_path=path
            )
            val = self._extract_value(response)
            if val is not None:
                return val
            else:
                raise AttributeError(f"Could not extract value from secret object: {type(response)}")

        except Exception as e:
            # Try lowercase fallback if not already tried
            if secret_name.isupper() or "_" in secret_name:
                try:
                    response = self.client.secrets.get_secret_by_name(
                        secret_name=secret_name.lower(),
                        project_id=self.project_id, 
                        environment_slug=env,
                        secret_path=path
                    )
                    val = self._extract_value(response)
                    if val is not None:
                        return val
                except Exception:
                    pass
            
            print(f"❌ Failed to fetch secret '{secret_name}' (Env: {env}): {e}")
            return None

    def list_secrets(self, environment=None, path="/"):
        """
        Lists all secrets in the project.
        """
        if not self.is_connected:
            return []
        
        env = environment or self.default_env
        
        try:
            response = self.client.secrets.list_secrets(
                project_id=self.project_id,
                environment_slug=env,
                secret_path=path
            )
            # The new SDK returns a ListSecretsResponse; extract the secrets list
            if hasattr(response, 'secrets'):
                return response.secrets
            return response
        except Exception as e:
            print(f"❌ Failed to list secrets (Env: {env}): {e}")
            return []

    def get_marketaux_keys(self):
        """
        Helper to fetch keys. Dynamically finds all secrets starting with 'marketaux-' or 'marketaux_'.
        """
        keys = []
        
        # Dynamic Discovery (The primary and only way)
        try:
            all_secrets = self.list_secrets()
            for s in all_secrets:
                key_name = self._extract_key_name(s)
                if not key_name:
                    continue
                
                # Check for both prefixes
                k_lower = key_name.lower()
                if k_lower.startswith("marketaux-") or k_lower.startswith("marketaux_"):
                    val = self._extract_value(s)
                    if not val:
                        # Fallback: Fetch explicitly if value not in list response
                        val = self.get_secret(key_name)
                    
                    if val:
                        keys.append(val)
                        
            if keys:
                print(f"🔑 Found {len(keys)} MarketAux keys via dynamic discovery.")
            
        except Exception as e:
            print(f"⚠️ Dynamic discovery failed: {e}")

        return list(set(keys)) # Dedup just in case


    def get_discord_webhook(self):
        """
        Fetches the Discord Webhook URL for notifications.
        """
        # Try specific user names, then fallback
        names = [
            "discord_captain_raw_news_webhook_url",
            "discord_captain_news_webhook_url",
            "discord_news_harvest_cli_webhook_url",
            "discord_data_harvest_cli_webhook_url",
            "discord_harvest_cli_webhook_url",
            "DISCORD_WEBHOOK_URL"
        ]
        for name in names:
            val = self.get_secret(name)
            if val: return val
        return None

    def get_turso_news_credentials(self):
        """
        Fetches Turso News DB credentials.
        Returns: (db_url, auth_token)
        """
        db_url = self.get_secret("turso_emadarshadalam_newsdatabase_DB_URL")
        auth_token = self.get_secret("turso_emadarshadalam_newsdatabase_AUTH_TOKEN")
        
        # Helper: Ensure compatibility (libsql:// -> https://)
        if db_url and "libsql://" in db_url:
            db_url = db_url.replace("libsql://", "https://")
            
        return db_url, auth_token

    def get_turso_analyst_credentials(self):
        """
        Fetches Turso Analyst DB credentials.
        Returns: (db_url, auth_token)
        """
        db_url = self.get_secret("turso_emadprograms_analystworkbench_DB_URL")
        auth_token = self.get_secret("turso_emadprograms_analystworkbench_AUTH_TOKEN")
        
        if db_url and "libsql://" in db_url:
            db_url = db_url.replace("libsql://", "https://")
            
        return db_url, auth_token

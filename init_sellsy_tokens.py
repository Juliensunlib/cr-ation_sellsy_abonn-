#!/usr/bin/env python3
"""
Script d'initialisation des tokens OAuth2 pour l'API Sellsy V2.
Ce script obtient un premier jeu de tokens d'accès et de rafraîchissement
à l'aide des identifiants client OAuth2.
"""

import os
import json
import requests
import argparse
import sys
from dotenv import load_dotenv

def setup_args():
    """Configure les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Obtient les tokens OAuth2 pour l'API Sellsy V2"
    )
    parser.add_argument(
        "--client_id", 
        help="ID client OAuth2"
    )
    parser.add_argument(
        "--client_secret", 
        help="Secret client OAuth2"
    )
    parser.add_argument(
        "--env_file", 
        default=".env", 
        help="Chemin vers le fichier .env (défaut: .env)"
    )
    parser.add_argument(
        "--update_env", 
        action="store_true",
        help="Mettre à jour le fichier .env avec les nouveaux tokens"
    )
    parser.add_argument(
        "--ci_mode",
        action="store_true",
        help="Mode CI: écrit les tokens directement dans les variables d'environnement"
    )
    
    return parser.parse_args()

def get_tokens(client_id, client_secret):
    """
    Obtient les tokens OAuth2 pour l'API Sellsy V2.
    
    Args:
        client_id: ID client OAuth2
        client_secret: Secret client OAuth2
        
    Returns:
        dict: Tokens d'accès et de rafraîchissement, ou None en cas d'erreur
    """
    # URL pour l'obtention des tokens
    auth_url = "https://login.sellsy.com/oauth2/access-tokens"
    
    # Préparation du payload pour la requête
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    
    # En-têtes pour spécifier le type de contenu
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    print(f"🔄 Envoi de la requête d'authentification à {auth_url}...")
    
    try:
        # Requête pour obtenir les tokens
        response = requests.post(
            auth_url, 
            json=payload,
            headers=headers,
            timeout=30
        )
        
        print(f"📝 Code de statut: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Tokens obtenus avec succès!")
            print(f"📊 Détails:")
            print(f"  - Access Token (premières lettres): {data['access_token'][:10]}...")
            print(f"  - Expire dans: {data.get('expires_in', 'N/A')} secondes")
            return data
        else:
            print(f"❌ Erreur lors de l'obtention des tokens: {response.status_code}")
            print(f"Détails: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Erreur: {str(e)}")
        return None

def update_env_file(env_file, access_token, refresh_token=None):
    """
    Met à jour le fichier .env avec les nouveaux tokens.
    
    Args:
        env_file: Chemin vers le fichier .env
        access_token: Token d'accès à ajouter
        refresh_token: Token de rafraîchissement à ajouter (optionnel)
    """
    print(f"🔄 Mise à jour du fichier {env_file}...")
    
    # Lire le contenu actuel du fichier
    try:
        with open(env_file, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"⚠️ Le fichier {env_file} n'existe pas, création...")
        lines = []
    
    # Indicateurs pour savoir si les tokens ont été trouvés et mis à jour
    access_token_found = False
    refresh_token_found = False
    
    # Mise à jour des tokens existants
    updated_lines = []
    for line in lines:
        if line.startswith('SELLSY_ACCESS_TOKEN='):
            updated_lines.append(f"SELLSY_ACCESS_TOKEN={access_token}\n")
            access_token_found = True
        elif line.startswith('SELLSY_REFRESH_TOKEN=') and refresh_token:
            updated_lines.append(f"SELLSY_REFRESH_TOKEN={refresh_token}\n")
            refresh_token_found = True
        else:
            updated_lines.append(line)
    
    # Ajout des tokens s'ils n'ont pas été trouvés
    if not access_token_found:
        updated_lines.append(f"SELLSY_ACCESS_TOKEN={access_token}\n")
    if not refresh_token_found and refresh_token:
        updated_lines.append(f"SELLSY_REFRESH_TOKEN={refresh_token}\n")
    
    # Écriture du contenu mis à jour
    with open(env_file, 'w') as f:
        f.writelines(updated_lines)
    
    print(f"✅ Fichier {env_file} mis à jour avec succès!")

def set_github_env_vars(access_token, refresh_token=None):
    """
    Configure les variables d'environnement pour GitHub Actions.
    
    Args:
        access_token: Token d'accès à définir
        refresh_token: Token de rafraîchissement (optionnel)
    """
    print("🔄 Configuration des variables d'environnement GitHub Actions...")
    
    # Dans GitHub Actions, on utilise le fichier GITHUB_ENV pour définir des variables
    github_env = os.environ.get('GITHUB_ENV')
    
    if github_env:
        with open(github_env, 'a') as f:
            f.write(f"SELLSY_ACCESS_TOKEN={access_token}\n")
            if refresh_token:
                f.write(f"SELLSY_REFRESH_TOKEN={refresh_token}\n")
        print("✅ Variables d'environnement GitHub définies avec succès!")
    else:
        # Si nous ne sommes pas dans GitHub Actions, définir directement
        os.environ["SELLSY_ACCESS_TOKEN"] = access_token
        if refresh_token:
            os.environ["SELLSY_REFRESH_TOKEN"] = refresh_token
        print("✅ Variables d'environnement définies avec succès!")

def main():
    """Fonction principale."""
    # Analyser les arguments
    args = setup_args()
    
    # Charger les variables d'environnement
    load_dotenv(args.env_file)
    
    # Récupérer les identifiants client depuis les arguments ou l'environnement
    client_id = args.client_id or os.environ.get("SELLSY_CLIENT_ID")
    client_secret = args.client_secret or os.environ.get("SELLSY_CLIENT_SECRET")
    
    # Vérifier que les identifiants sont définis
    if not client_id or not client_secret:
        print("❌ Les identifiants client sont manquants!")
        print("Veuillez les spécifier via les arguments ou dans le fichier .env")
        return 1
    
    # Obtenir les tokens
    result = get_tokens(client_id, client_secret)
    
    if not result:
        print("❌ Impossible d'obtenir les tokens")
        return 1
    
    # Extraire les tokens
    access_token = result.get("access_token")
    refresh_token = result.get("refresh_token")  # Peut être None avec client_credentials
    
    # Afficher les instructions
    print("\n📋 Tokens d'accès:")
    print(f"Access Token: {access_token[:10]}...")
    if refresh_token:
        print(f"Refresh Token: {refresh_token[:10]}...")
    else:
        print("Note: Aucun refresh token n'a été fourni avec le flux client_credentials")
    
    # Mettre à jour le fichier .env si demandé
    if args.update_env:
        update_env_file(args.env_file, access_token, refresh_token)
    
    # Définir les variables d'environnement pour GitHub Actions
    if args.ci_mode or 'GITHUB_ACTIONS' in os.environ:
        set_github_env_vars(access_token, refresh_token)
    
    if not args.update_env and not args.ci_mode and 'GITHUB_ACTIONS' not in os.environ:
        print("\n🔔 Pour utiliser ces tokens:")
        print(f"1. Ajoutez manuellement ces tokens à votre fichier {args.env_file}")
        print("2. Ou exécutez à nouveau ce script avec l'option --update_env")
    
    return 0

if __name__ == "__main__":
    exit(main())

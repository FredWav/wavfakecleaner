# Wav Fake Cleaner (GUI)

Outil de nettoyage automatisé pour Threads. Ce programme identifie les comptes inactifs ou automatisés (bots) via un système de scoring propriétaire et permet leur suppression sécurisée.

## Installation

- **Python 3.10+** requis.
- **Dépendances** :
```bash
pip install -r requirements.txt
playwright install chromium
```

## Lancement

```bash
python app.py
```

## Workflow Stratégique

L'ordre des opérations suit la structure de l'interface pour garantir l'intégrité de la base de données :

1.  - **🚀 Lancer Chrome** : Ouvre une instance sécurisée pilotée par CDP sur le port 9222. Connecte-toi manuellement à ton compte Threads dans cette fenêtre.
2.  - **📥 Récupérer abonnés** : Extraction hybride via l'API interne (vitesse maximale) avec fallback par défilement automatique (scroll).
3.  - **🔍 Scanner les profils** : Analyse granulaire de chaque compte (bio, activité, photo, posts récents) pour attribuer un score de 0 à 100.
4.  - **🧹 Nettoyer les fakes** : Suppression ou blocage définitif des comptes dépassant le seuil critique (recommandé : 70/100).
5.  - **🤖 Autopilot** : Automatisation complète des cycles de scan et de nettoyage avec détection des re-follows.

## Profils de Sécurité (Anti-Ban)

Les limites sont calées sur les seuils de détection algorithmique de Threads :

| Profil       | Actes /jour | Actes /heure | Pause Clean | Batch Scan |
| :---         | :--         | :--          | :--         | :--        |
| **Prudent**  | 160         | 25           | 15 - 30s    | 80         |
| **Normal**   | 300         | 40           | 8 - 15s     | 120        |
| **Agressif** | 500         | 50           | 5 - 10s     | 150        |

## Fichiers et Données

- - **`followers_db.json`** : Base de données locale de tes abonnés et scores.
- - **`actions.log`** : Historique complet des opérations effectuées.
- - **`export.csv`** : Rapport détaillé prêt pour analyse externe.
- - **`chrome_profile/`** : Dossier de session Chrome (à ne jamais partager).
- - **`debug_*.png`** : Captures d'écran générées automatiquement en cas d'erreur visuelle sur Threads.

---
*Développé par Fred Wav — Focus sur la valeur mesurable et l'efficacité radicale.*

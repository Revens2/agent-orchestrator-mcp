# Recovery temporaire d’un runner dupliqué

`ORCH_RUNNER_SOURCE_BINDINGS` est un mécanisme de **migration/récupération uniquement**. Il sert lorsqu’un second PC a accidentellement reçu le même `runner_id` **et le même token** qu’un runner existant, ce qui provoque des `hello` alternés, des epochs qui se supplantent et des jobs `lost`.

Syntaxe :

```text
ORCH_RUNNER_SOURCE_BINDINGS=base_runner_id@ip_source=target_runner_id
```

Exemple temporaire :

```text
main-windows-pc@10.200.160.243=pc-fixe
```

## Garanties

Le binding n’est appliqué **qu’après** :

1. validation de `X-Real-IP` dans `ORCH_RUNNER_CIDRS` ;
2. validation du bearer token contre `ORCH_RUNNER_TOKENS` ;
3. dérivation de l’identité de base depuis ce token.

L’IP ne constitue jamais une authentification. Un token invalide reste `401` même depuis une IP liée. Une source hors CIDR reste `403`. Un autre token n’est pas affecté par le binding.

## Procédure

1. Identifier avec certitude l’IP overlay de la machine dupliquée.
2. Sauvegarder la configuration broker.
3. Ajouter le binding temporaire et redémarrer `orch-mcp`.
4. Vérifier que les deux runners apparaissent séparément.
5. Sur le runner récupéré, générer un nouveau token cryptographiquement aléatoire et passer son `runner_id` définitif.
6. Ajouter uniquement le SHA-256 du nouveau token à `ORCH_RUNNER_TOKENS`.
7. Redémarrer le runner récupéré avec son token unique.
8. Retirer `ORCH_RUNNER_SOURCE_BINDINGS`, redémarrer le broker et vérifier que les deux runners restent stables.

Ne pas conserver ce binding comme architecture permanente : l’état final normal est **un token unique par runner**.

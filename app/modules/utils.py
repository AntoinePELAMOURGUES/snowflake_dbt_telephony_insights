import re


def validate_password_strength(password):
    """
    Valide que le mot de passe respecte les critères de l'ANSSI.
    Retourne (True, "") si OK, sinon (False, "Message d'erreur").
    """
    # 1. Vérification de la longueur
    if len(password) < 12:
        return False, "Le mot de passe doit contenir au moins 12 caractères."

    # 2. Vérification Majuscule
    if not re.search(r"[A-Z]", password):
        return False, "Le mot de passe doit contenir au moins une majuscule."

    # 3. Vérification Minuscule
    if not re.search(r"[a-z]", password):
        return False, "Le mot de passe doit contenir au moins une minuscule."

    # 4. Vérification Chiffre
    if not re.search(r"\d", password):
        return False, "Le mot de passe doit contenir au moins un chiffre."

    # 5. Vérification Caractère Spécial
    if not re.search(r"[ !@#$%^&*()_+\-=\[\]{};':\"\\|,.<>\/?]", password):
        return False, "Le mot de passe doit contenir au moins un caractère spécial."

    return True, ""

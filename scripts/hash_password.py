import bcrypt
import getpass

# 1. Définissez le mot de passe souhaité
password_to_hash = getpass.getpass(
    "Entrez le mot de passe pour le premier utilisateur : "
)

# 2. Générer le hash
# encode() transforme la string en bytes, ce que bcrypt attend
hashed_bytes = bcrypt.hashpw(password_to_hash.encode("utf-8"), bcrypt.gensalt())

# 3. decode() re-transforme les bytes en string pour stockage en BDD
hashed_string = hashed_bytes.decode("utf-8")

print("\n--- Hash à copier dans Snowflake ---")
print(hashed_string)
print("--------------------------------------")

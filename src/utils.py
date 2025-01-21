prompt = """
        I need a list of the 5 most similar films to {}, ranked by similarity. 
        Focus on factors such as:
        - Genre (e.g., sci-fi, drama, action)
        - Tone and Atmosphere (e.g., dark, lighthearted, suspenseful)
        - Themes (e.g., redemption, survival, family)
        - Plot Elements (e.g., time travel, heists, coming-of-age)
        - Character Dynamics (e.g., mentor-student, rivalries, ensemble casts)
        - Visual Style (if applicable, e.g., stylized, realistic, animated)

        Provide the list in descending order of similarity, with a short explanation for each recommendation highlighting the key overlapping elements with '<film-title>'.
        """
        
def build_prompt(search_string):
        return prompt.format(search_string)
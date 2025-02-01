import base64
import json
import time
import os
from typing import Dict, List
from google import genai
from google.genai import types
from pydantic import BaseModel
from openai import OpenAI

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.models.param import Param


with DAG(
    dag_id="generate_book",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={"story_description": Param(type="string")},
) as dag:

    @task
    def generate_story(dag_run):
        prompt = f"""
        Write a short story aimed at toddlers, using the following description:
        
        ```
        {dag_run.conf['story_description']}
        ```
        
        Make sure the story contains a life lesson.  Keep the story positive.
        
        Make sure the sentences are not too long, and ensure simple words are used.
        
        The output should be eight paragraphs long.
        
        Your output should be a JSON object containing an array of 8 paragraphs.
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema={"type": "array", "items": {"type": "string"}},
            ),
        )

        out = json.loads(response.text)
        print(json.dumps(out, indent=2))

        return out

    generate_story()


# class StoryImageGenerator:
#     def __init__(self):
#         self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
#         self.openai_client = OpenAI()

#     def understand_story_genai(self, story_text: str) -> str:
#         print("Agent (GenAI): Understanding the story...")
#         prompt = f"""
#         Please read the following children's story and then:
#         1. Provide a concise summary of the story in 1-2 sentences.
#         2. Identify the overall theme or central message of the story in a single sentence.

#         Story:
#         ```
#         {story_text}
#         ```
#         """

#         class StoryInfo(BaseModel):
#             summary: str
#             theme: str

#         for _ in range(0, 10):
#             try:
#                 response = self.client.models.generate_content(
#                     model="gemini-2.0-flash-exp",
#                     contents=prompt,
#                     config=types.GenerateContentConfig(
#                         response_mime_type="application/json", response_schema=StoryInfo
#                     ),
#                 )
#                 break
#             except Exception as e:
#                 print(str(e))
#                 time.sleep(10)

#         response_json = json.loads(response.text)

#         summary = response_json["summary"]
#         theme = response_json["theme"]

#         return f"{summary}\n{theme}"

#     def get_character_descriptions_genai(self, story_text: str) -> List[Dict]:
#         print("Agent (GenAI): Creating detailed character descriptions...")
#         prompt = f"""
#         Please read the following children's story and then:
#         1. Identify all the main characters in the story.  Characters don't have to be animate; any recurring item or creature is a character.
#         2. For each character, create a detailed visual description that can be used to consistently represent them in images.
#            Describe in detail all features of the character - do not overlook minor features.
#            Do not use generic terms: make sure every feature is explicitly described and detailed.
#            Fill in the blanks on your own if features are "unspecified" or "unknown".  Be as descriptive as possible.
#            Do not use generic terms such as "young" or "old"; be explicit in age.
#         3. Focus on physical appearance, key attributes, and any distinguishing features mentioned or implied in the story.
#         4. Do not describe any expressions, or postures.  We want a description that can be used as a base, which can be built upon throughout the story.

#         Story:
#         ```
#         {story_text}
#         ```

#         Output the results as key/value pairs of character names and descriptions.
#         """

#         class CharacterDescription(BaseModel):
#             character_name: str
#             character_description: str

#         for _ in range(0, 10):
#             try:
#                 response = self.client.models.generate_content(
#                     model="gemini-2.0-flash-exp",
#                     contents=prompt,
#                     config=types.GenerateContentConfig(
#                         response_mime_type="application/json",
#                         response_schema=list[CharacterDescription],
#                     ),
#                 )
#                 break
#             except Exception as e:
#                 print(str(e))
#                 time.sleep(10)

#         character_descriptions = json.loads(response.text)
#         print(json.dumps(character_descriptions, indent=2))
#         return character_descriptions

#     def get_paragraph_description(self, paragraph: str, story_summary: str) -> Dict:
#         prompt = f"""Provide a scene description suitable for generating an image, focusing on the actions, expressions, and postures of the characters. Describe the characters' actions and postures to convey their emotions, rather than explicitly stating the emotions.  The character descriptions will be provided separately, so make sure to refer to the characters by name.

#         Use the following scene as context:
#         ```
#         {paragraph}
#         ```

#         This scene is part of the following story summary:
#         ```
#         {story_summary}
#         ```

#         The output should be a JSON object with two keys:
#         ```json
#         {{
#             "focus": "a description of the overall scene; environment.",
#             "action": "a description of the characters' actions and postures."
#         }}
#         ```
#         """

#         class ParagraphDescription(BaseModel):
#             focus: str
#             action: str

#         for _ in range(0, 10):
#             try:
#                 response = self.client.models.generate_content(
#                     model="gemini-2.0-flash-exp",
#                     contents=prompt,
#                     config=types.GenerateContentConfig(
#                         response_mime_type="application/json",
#                         response_schema=ParagraphDescription,
#                     ),
#                 )
#                 break
#             except Exception as e:
#                 print(str(e))
#                 time.sleep(10)

#         paragraph_description = json.loads(response.text)
#         return paragraph_description

#     def generate_image_prompt(
#         self, characters: List[Dict], theme: str, paragraph_text: str
#     ) -> str:
#         present_characters = []
#         paragraph_lower = paragraph_text.lower()
#         for char_data in characters:
#             char_name = char_data["character_name"]
#             if char_name.lower() in paragraph_lower or paragraph_lower == "the end.":
#                 present_characters.append(char_data)

#         paragraph_description = self.get_paragraph_description(paragraph_text, theme)

#         character_descriptions = ""
#         if present_characters:
#             formatted_list = "\n".join(
#                 f"- {c['character_name']}: {c['character_description']}"
#                 for c in present_characters
#             )
#             character_descriptions = f"""Character Descriptions:\n{formatted_list}"""

#         image_prompt = f"""Render the following in an oil painting style:

#         {character_descriptions}

#         Focus: {paragraph_description['focus']}

#         Action: {paragraph_description['action']}

#         Render it in an oil painting style.

#         It should be a single image, detailing the characters and environment, and nothing else.

#         I NEED to test how the tool works with extremely simple prompts. DO NOT add any detail, just use it AS-IS.
#         """

#         return image_prompt

#     def generate_scene_description_genai(
#         self,
#         paragraph_text: str,
#         story_theme: str,
#         character_descriptions: List[Dict],
#         paragraph_number: int,
#     ) -> str:
#         prompt = self.generate_image_prompt(
#             character_descriptions, story_theme, paragraph_text
#         )
#         print(f"IMAGE PROMPT {paragraph_number}: {prompt}")
#         return prompt

#     def process_story_for_prompts_genai(self, paragraphs) -> List[Dict]:
#         story_text = "  ".join(paragraphs)

#         story_theme = self.understand_story_genai(story_text)
#         character_descriptions = self.get_character_descriptions_genai(story_text)
#         paragraphs.append("The end.")

#         scene_prompts = []
#         for i, paragraph in enumerate(paragraphs):
#             prompt = self.generate_scene_description_genai(
#                 paragraph, story_theme, character_descriptions, i + 1
#             )
#             scene_prompts.append({"paragraph": paragraph, "prompt": prompt})

#         return scene_prompts

#     def generate_story(self, story_description):
#         prompt = f"""
#         Write a short story aimed at toddlers, using the following description:

#         ```
#         {story_description}
#         ```

#         Make sure the story contains a life lesson.  Keep the story positive.

#         Make sure the sentences are not too long, and ensure simple words are used.

#         The output should be eight paragraphs long.

#         Your output should be a JSON object containing an array of 8 paragraphs.
#         """

#         for _ in range(0, 10):
#             try:
#                 response = self.client.models.generate_content(
#                     model="gemini-2.0-flash-exp",
#                     contents=prompt,
#                     config=types.GenerateContentConfig(
#                         response_mime_type="application/json",
#                         response_schema={"type": "array", "items": {"type": "string"}},
#                     ),
#                 )
#                 break
#             except Exception as e:
#                 print(str(e))
#                 time.sleep(10)

#         out = json.loads(response.text)
#         print(json.dumps(out, indent=2))

#         return out

#     def generate(self, title: str, story_description: str) -> None:
#         story = self.generate_story(story_description)
#         scene_prompts_genai = self.process_story_for_prompts_genai(story)

#         directory = f"/home/dan/git/book/static/{title}"
#         os.makedirs(directory, exist_ok=True)

#         for i, prompt_obj in enumerate(scene_prompts_genai):
#             print(f"generating image for paragraph {i}")

#             for _ in range(5):
#                 try:
#                     response = self.openai_client.images.generate(
#                         model="dall-e-3",
#                         prompt=prompt_obj["prompt"],
#                         size="1024x1024",
#                         quality="standard",
#                         n=1,
#                         style="vivid",
#                         response_format="b64_json",
#                     )
#                     break
#                 except Exception as e:
#                     print(str(e))

#             page_dir = f"{directory}/{i}"
#             os.makedirs(page_dir, exist_ok=True)

#             with open(f"{page_dir}/image.jpg", "wb") as f:
#                 image_data = base64.b64decode(response.data[0].b64_json)
#                 f.write(image_data)

#             with open(f"{page_dir}/text.txt", "w", encoding="utf-8") as f:
#                 f.write(prompt_obj["paragraph"])

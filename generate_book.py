import base64
from datetime import timedelta
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
    params={
        "title": Param("", type="string"),
        "story_description": Param(type="string"),
    },
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
) as dag:

    @task
    def generate_story(dag_run):
        prompt = f"""
        Write a short story aimed at toddlers, using the following description:
        
        ```
        {dag_run.conf['story_description']}
        ```
        
        Make sure the story contains a life lesson, and a humorous twist.  Keep the story positive.  Use simple words.
        
        The story should be eight paragraphs long.
        
        Your output should be a JSON object containing a title and an array of eight paragraphs.
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        class Story(BaseModel):
            title: str
            story: list[str]

        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=Story,
            ),
        )

        out = json.loads(response.text)
        return [out["title"], *out["story"], "The end."]

    @task
    def understand_story(story) -> str:
        prompt = f"""
        Read the following children's story and then:

        1. Provide a concise summary of the story in 1-2 sentences.
        2. Identify the overall theme or central message of the story in a single sentence.

        Story:
        ```
        {json.dumps(story, indent=2)}
        ```
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        class StoryInfo(BaseModel):
            summary: str
            theme: str

        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json", response_schema=StoryInfo
            ),
        )

        response_json = json.loads(response.text)

        return response_json

    @task
    def get_character_descriptions(story) -> List[Dict]:
        prompt = f"""
        Please read the following children's story and then:
        1. Identify all the main characters in the story.  Characters don't have to be animate; any recurring item or creature is a character.
        2. For each character, create a detailed visual description that can be used to consistently represent them in images.
           Describe in detail all features of the character - do not overlook minor features.
           Do not use generic terms: make sure every feature is explicitly described and detailed.
           For example, say "bear cub" if it's a young bear.  Specify the breed of animal, eg. dalmation for a dog, or triceratops for a dinosaur.
           Fill in the blanks on your own if features are "unspecified" or "unknown".  Be as descriptive as possible.
           Do not use generic terms such as "young" or "old"; be explicit in age.
        3. Focus on physical appearance, key attributes, and any distinguishing features mentioned or implied in the story.
        4. Do not describe any expressions, or postures.  We want a description that can be used as a base, which can be built upon throughout the story.

        Story:
        ```
        {json.dumps(story, indent=2)}
        ```
        
        Output the results as key/value pairs of character names and descriptions.
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        class CharacterDescription(BaseModel):
            character_name: str
            character_description: str

        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=list[CharacterDescription],
            ),
        )

        character_descriptions = json.loads(response.text)
        return character_descriptions

    @task
    def get_paragraph_description(paragraph, summary, characters):
        prompt = f"""Provide a scene description suitable for generating an image, focusing on the actions, expressions, and postures of the characters. Describe the characters' actions and postures to convey their emotions, rather than explicitly stating the emotions.  The character descriptions will be provided separately, so make sure to refer to the characters by name. 

        Use the following scene as context:
        ```
        {paragraph}
        ```

        This scene is part of the following story summary:
        ```json
        {json.dumps(summary, indent=2)}
        ```
        
        Characters of note are as follows - the output must refer to these characters by character_name when described.
        ```json
        {json.dumps(characters, indent=2)}
        ```

        The output should be a JSON object with two keys:
        ```json
        {{
            "focus": "a description of the overall scene; environment.",
            "action": "a description of the characters' actions and postures."
        }}
        ```
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        class ParagraphDescription(BaseModel):
            focus: str
            action: str

        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=ParagraphDescription,
            ),
        )

        paragraph_description = json.loads(response.text)
        return {
            "paragraph_text": paragraph,
            "paragraph_description": paragraph_description,
        }

    @task
    def generate_image_prompt(
        characters,
        paragraph_text,
        paragraph_description,
    ):
        present_characters = []
        paragraph_lower = paragraph_text.lower()
        for char_data in characters:
            char_name = char_data["character_name"]
            if (
                char_name.lower() in paragraph_description["focus"].lower()
                or char_name.lower() in paragraph_description["action"].lower()
                or paragraph_lower == "the end."
            ):
                present_characters.append(char_data)

        character_descriptions = ""
        if present_characters:
            formatted_list = "\n".join(
                f"- {c['character_name']}: {c['character_description']}"
                for c in present_characters
            )
            character_descriptions = f"""Character Descriptions:\n{formatted_list}"""

        image_prompt = f"""Render the following in an oil painting style:

        {character_descriptions}

        Focus: {paragraph_description['focus']}

        Action: {paragraph_description['action']}

        Render it in an oil painting style.

        It should be a single image, detailing the characters and environment, and nothing else.

        I NEED to test how the tool works with extremely simple prompts. DO NOT add any detail, just use it AS-IS.
        """

        return {"image_prompt": image_prompt, "paragraph_text": paragraph_text}

    @task
    def generate_image(story, prompt_data, dag_run, task_instance):
        import re

        root_dir = re.sub(r"^[a-z_]", "", story[0].lower().replace(" ", "_"))
        page_dir = f"/media/seagate/flask-static/book/static/{root_dir}/{task_instance.map_index}"
        os.makedirs(page_dir, exist_ok=True)

        conn = BaseHook.get_connection("openai_api")
        response = OpenAI(api_key=conn.password).images.generate(
            model="dall-e-3",
            prompt=prompt_data["image_prompt"],
            size="1024x1024",
            quality="standard",
            n=1,
            style="vivid",
            response_format="b64_json",
        )

        with open(f"{page_dir}/image.jpg", "wb") as f:
            image_data = base64.b64decode(response.data[0].b64_json)
            f.write(image_data)

        with open(f"{page_dir}/text.txt", "w", encoding="utf-8") as f:
            f.write(prompt_data["paragraph_text"])

    story = generate_story()
    summary = understand_story(story)
    character_descriptions = get_character_descriptions(story)

    paragraph_descriptions = get_paragraph_description.partial(
        summary=summary, characters=character_descriptions
    ).expand(paragraph=story)

    prompt_data = generate_image_prompt.partial(
        characters=character_descriptions
    ).expand_kwargs(paragraph_descriptions)

    generate_image.partial(story=story).expand(prompt_data=prompt_data)

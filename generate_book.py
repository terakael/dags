import base64
from datetime import timedelta
import json
import random
import re
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

text_model = "gemini-2.0-pro-exp-02-05"
image_model = "imagen-3.0-generate-002"

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
    def select_story_structure():
        """Randomly select a story structure to guide the narrative."""
        # Define various story structures
        story_structures = [
            {
                "name": "Hero's Journey",
                "description": "A character goes on an adventure, faces challenges, and returns transformed",
                "elements": [
                    "Ordinary World",
                    "Call to Adventure",
                    "Meeting the Mentor",
                    "Crossing the Threshold",
                    "Tests, Allies, and Enemies",
                    "The Ordeal",
                    "Reward",
                    "Return with the Elixir",
                ],
            },
            {
                "name": "Three-Act Structure",
                "description": "Setup, confrontation, and resolution",
                "elements": [
                    "Exposition",
                    "Inciting Incident",
                    "Rising Action",
                    "Midpoint",
                    "Complications",
                    "Climax",
                    "Resolution",
                ],
            },
            {
                "name": "Problem-Solution",
                "description": "Character faces a problem and finds a solution",
                "elements": [
                    "Introduce Character",
                    "Establish Problem",
                    "Failed Attempts",
                    "Discovery",
                    "Implementing Solution",
                    "Resolution",
                ],
            },
            {
                "name": "Character Transformation",
                "description": "Character undergoes a significant change",
                "elements": [
                    "Initial Character State",
                    "Catalyst for Change",
                    "Resistance",
                    "Turning Point",
                    "Growth",
                    "New Character State",
                ],
            },
            {
                "name": "Quest/Adventure",
                "description": "Characters embark on a journey to achieve a goal",
                "elements": [
                    "The Call",
                    "Preparation",
                    "Journey Begins",
                    "Obstacles",
                    "Final Challenge",
                    "Achievement",
                    "Return",
                ],
            },
        ]

        # Randomly select a story structure
        selected_structure = random.choice(story_structures)

        # Log the selected structure
        print(f"Selected story structure: {selected_structure['name']}")

        return selected_structure

    @task
    def define_characters(story_structure, dag_run):
        """Create detailed character definitions based on the story description and selected structure."""
        prompt = f"""
        Based on the following story description and selected story structure, define 2-4 main characters for a children's story for toddlers.
        
        Story Description:
        ```
        {dag_run.conf["story_description"]}
        ```
        
        Story Structure: {story_structure['name']}
        Structure Elements: {', '.join(story_structure['elements'])}
        
        For each character, provide:
        1. Name
        2. Type (animal, person, magical creature, object, etc.)
        3. Key traits (3-5 personality traits)
        4. Role in the story (protagonist, helper, antagonist, etc.)
        5. Motivation (what drives this character)
        6. Physical description (brief but specific)
        
        Make the characters appropriate for toddlers - simple, relatable, and engaging.
        
        Output in JSON format with each character as an object in an array.
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        class Character(BaseModel):
            name: str
            type: str
            traits: list[str]
            role: str
            motivation: str
            physical_description: str

        response = client.models.generate_content(
            model=text_model,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=list[Character],
                temperature=1.0,
            ),
        )

        characters = json.loads(response.text)
        return characters

    @task
    def plot_story_arc(story_structure, characters, dag_run):
        """Develop a plot arc following the selected story structure."""
        characters_json = json.dumps(characters, indent=2)

        prompt = f"""
        Create a plot arc for a children's story based on the following information:
        
        Story Description:
        ```
        {dag_run.conf["story_description"]}
        ```
        
        Story Structure: {story_structure['name']}
        Structure Elements: {', '.join(story_structure['elements'])}
        
        Characters:
        ```json
        {characters_json}
        ```
        
        For each element in the story structure, provide:
        1. A brief description of what happens in that part of the story
        2. Which characters are involved
        3. How this advances the overall narrative
        
        Remember:
        - The story is for toddlers, so keep it simple and positive
        - Include a life lesson or moral
        - Add a humorous twist somewhere in the story
        - Use age-appropriate conflict and resolution
        
        Output in JSON format with each structure element as a key and the details as values.
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        response = client.models.generate_content(
            model=text_model,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=1.0,
            ),
        )

        story_arc = json.loads(response.text)
        return story_arc

    @task
    def create_story_outline(story_structure, characters, story_arc, dag_run):
        """Create a detailed paragraph-by-paragraph outline of the story."""
        characters_json = json.dumps(characters, indent=2)
        story_arc_json = json.dumps(story_arc, indent=2)

        prompt = f"""
        Create a detailed paragraph-by-paragraph outline for a children's story based on the following information:
        
        Story Description:
        ```
        {dag_run.conf["story_description"]}
        ```
        
        Story Structure: {story_structure['name']}
        
        Characters:
        ```json
        {characters_json}
        ```
        
        Story Arc:
        ```json
        {story_arc_json}
        ```
        
        Guidelines:
        1. Create between 8 and 20 paragraphs
        2. Each paragraph should be a logical scene or beat in the story
        3. Determine the appropriate number of paragraphs based on the complexity of the story
        4. For each paragraph, provide a brief description of what happens
        5. Indicate which characters appear in each paragraph
        6. Show how the paragraph advances the story
        
        The outline will be used to generate the full story, so be specific about what happens in each paragraph.
        
        Output in JSON format with an array of paragraph outlines. Each paragraph outline should have:
        - a number (starting from 1)
        - a brief title
        - a description of what happens
        - characters involved
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        class ParagraphOutline(BaseModel):
            number: int
            title: str
            description: str
            characters: list[str]

        response = client.models.generate_content(
            model=text_model,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=list[ParagraphOutline],
                temperature=1.0,
            ),
        )

        outline = json.loads(response.text)
        return outline

    @task
    def generate_story(story_structure, characters, story_arc, story_outline, dag_run):
        """Generate the full story based on the planning information."""
        story_description = dag_run.conf["story_description"]
        characters_json = json.dumps(characters, indent=2)
        story_arc_json = json.dumps(story_arc, indent=2)
        story_outline_json = json.dumps(story_outline, indent=2)

        prompt = f"""
        Write a children's story for toddlers based on the following detailed plan:
        
        Story Description:
        ```
        {story_description}
        ```
        
        Story Structure: {story_structure['name']}
        
        Characters:
        ```json
        {characters_json}
        ```
        
        Story Arc:
        ```json
        {story_arc_json}
        ```
        
        Paragraph Outline:
        ```json
        {story_outline_json}
        ```
        
        Guidelines:
        1. Follow the paragraph outline exactly, creating one paragraph for each outline item
        2. Use simple words appropriate for toddlers
        3. Keep the story positive and engaging
        4. Include the life lesson as planned in the story arc
        5. Include the humorous twist as planned
        6. Make sure each character behaves according to their defined traits
        
        Your output should be a JSON object containing:
        1. A title for the story
        2. An array of paragraphs (one for each outline item)
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        class Story(BaseModel):
            title: str
            story: list[str]

        response = client.models.generate_content(
            model=text_model,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=Story,
                temperature=1.0,
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
            model=text_model,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=StoryInfo,
                temperature=2.0,
            ),
        )

        response_json = json.loads(response.text)

        return response_json

    @task
    def get_character_descriptions(story) -> List[Dict]:
        prompt = f"""
        Please read the following children's story and then:
        1. Identify all the main characters in the story (including important recurring items or creatures).
        2. For each character, create a detailed visual description that can be used to consistently represent them in images.
           IMPORTANT: Always explicitly specify the following even if not mentioned in the story:
           - Exact age/development stage (cub vs adult bear, puppy vs grown dog, etc.)
           - Precise species and subspecies where applicable
           - Specific colors, textures, and patterns
           - Size relative to other characters
           - Distinctive physical features
           
           Example: "A 3-year-old brown grizzly bear cub with honey-colored snout" NOT just "a bear"
           Example: "A bright red vintage fire truck with yellow ladder and chrome bumpers" NOT just "a fire truck"
           
           DO NOT use general terms like "young" or "old" - specify exact age or development stage.
           DO NOT leave any physical attribute unspecified - make definitive choices for all visual aspects.
           
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
            model=text_model,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=list[CharacterDescription],
                temperature=2.0,
            ),
        )

        character_descriptions = json.loads(response.text)
        return character_descriptions

    @task
    def get_paragraph_description(paragraph, summary, characters):
        prompt = f"""Create a concise scene description for image generation with these two components:
        1. The setting/environment
        2. The characters' actions and postures (refer to characters by name)

        Scene:
        ```
        {paragraph}
        ```

        Story context:
        ```json
        {json.dumps(summary, indent=2)}
        ```
        
        Characters:
        ```json
        {json.dumps(characters, indent=2)}
        ```

        Output as JSON:
        ```json
        {{
            "focus": "brief description of setting/environment",
            "action": "description of characters' actions, using as few words as possible."
        }}
        ```
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        class ParagraphDescription(BaseModel):
            focus: str
            action: str

        response = client.models.generate_content(
            model=text_model,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=ParagraphDescription,
                temperature=2.0,
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
            # More structured format that's both concise and readable
            char_entries = []
            for c in present_characters:
                # Trim any extra spaces and ensure the description is concise
                desc = c["character_description"].strip()
                char_entries.append(f"{c['character_name']}: {desc}")

            character_descriptions = "\n- ".join(char_entries)

        image_prompt = f"""
        # Image theme
        Oil painting style; single image showing characters and environment only.

        # Characters
        {character_descriptions}

        # Setting
        {paragraph_description['focus']}
        
        # Action
        {paragraph_description['action']}
        """

        return {"image_prompt": image_prompt, "paragraph_text": paragraph_text}

    @task
    def generate_image(story, prompt_data, task_instance):
        root_dir = re.sub(r"[^a-z_]+", "", story[0].lower().replace(" ", "_"))
        page_dir = f"/media/seagate/flask-static/book/static/{root_dir}/{task_instance.map_index}"
        os.makedirs(page_dir, exist_ok=True)

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        response = client.models.generate_images(
            model=image_model,
            prompt=prompt_data["image_prompt"],
            config=types.GenerateImagesConfig(number_of_images=1),
        )
        for generated_image in response.generated_images:
            with open(f"{page_dir}/image.jpg", "wb") as f:
                f.write(generated_image.image.image_bytes)

        # conn = BaseHook.get_connection("openai_api")
        # response = OpenAI(api_key=conn.password).images.generate(
        #     model="dall-e-3",
        #     prompt=prompt_data["image_prompt"],
        #     size="1024x1024",
        #     quality="standard",
        #     n=1,
        #     style="vivid",
        #     response_format="b64_json",
        # )

        # with open(f"{page_dir}/image.jpg", "wb") as f:
        #     image_data = base64.b64decode(response.data[0].b64_json)
        #     f.write(image_data)

        with open(f"{page_dir}/text.txt", "w", encoding="utf-8") as f:
            f.write(prompt_data["paragraph_text"])

    @task
    def create_thumbnail(story):
        from PIL import Image

        title_dir = re.sub(r"[^a-z_]+", "", story[0].lower().replace(" ", "_"))
        static_dir = f"/media/seagate/flask-static/book/static"

        img = Image.open(os.path.join(static_dir, title_dir, "0", "image.jpg"))
        new_size = (int(img.width * 0.2), int(img.height * 0.2))

        thumbnail = img.resize(new_size, Image.LANCZOS)
        thumbnail.save(os.path.join(static_dir, title_dir, "thumbnail.jpg"))

    # Chain-of-thought story planning
    story_structure = select_story_structure()

    characters = define_characters(
        story_structure=story_structure,
    )

    story_arc = plot_story_arc(
        story_structure=story_structure,
        characters=characters,
    )

    story_outline = create_story_outline(
        story_structure=story_structure,
        characters=characters,
        story_arc=story_arc,
    )

    # Generate the story based on the planning
    story = generate_story(
        story_structure=story_structure,
        characters=characters,
        story_arc=story_arc,
        story_outline=story_outline,
    )

    # Proceed with the rest of the pipeline
    summary = understand_story(story)
    character_descriptions = get_character_descriptions(story)

    paragraph_descriptions = get_paragraph_description.partial(
        summary=summary, characters=character_descriptions
    ).expand(paragraph=story)

    prompt_data = generate_image_prompt.partial(
        characters=character_descriptions
    ).expand_kwargs(paragraph_descriptions)

    images = generate_image.partial(story=story).expand(prompt_data=prompt_data)

    # The 0th image is the title image, so this will be the thumbnail.
    images >> create_thumbnail(story)

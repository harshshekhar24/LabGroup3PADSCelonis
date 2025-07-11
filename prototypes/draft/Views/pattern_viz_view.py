import pandas as pd
import streamlit as st
from prototypes.draft.functions import assign_node_positions, change_page
from pyvis.network import Network
import streamlit.components.v1 as components
from collections import defaultdict

"""
    # Load the dataset
    file_path = os.path.join(
        os.path.dirname(__file__), "EMMA_Pattern_Vorschau_JSON.csv"
    )
    df = pd.read_csv(file_path)
    df["episode"] = df["episode"].apply(ast.literal_eval)
"""


def pattern_viz_view():
    st.title("Frequent Episodes Visualization")

    if "episodes" not in st.session_state or not st.session_state.episodes:
        st.warning("No mined episodes found. Please run the mining step first.")
        if st.button("Back"):
            if st.session_state.operation_mode == "uploadCSV":
                change_page("pattern_mining")
        return

    # Load the dataset (CSV Example Import)
    # file_path = os.path.join(
    #     os.path.dirname(__file__), "EMMA_Pattern_Vorschau_JSON.csv"
    # )
    # df = pd.read_csv(file_path)
    # print(df)
    # df["episode"] = df["episode"].apply(ast.literal_eval)
    # df.rename(columns={"episode": "Episode"}, inplace=True)
    # df.rename(columns={"support": "Support"}, inplace=True)
    # df.rename(columns={"pattern_id": "PatternId"}, inplace=True)
    # print(df)

    # Convert episodes to DataFrame
    df_episodes = pd.DataFrame(st.session_state.episodes)
    df_episodes["PatternId"] = range(len(df_episodes))
    df_episodes["PatternId"] = df_episodes["PatternId"].astype(int)
    df_episodes["Support"] = df_episodes["Support"].astype(int)

    def process_episode(episode_list):
        # Parses Input & Adds Default Object to handle Case Centric Data
        processed_list = []
        for element in episode_list:
            new_element = element.copy()
            if isinstance(new_element.get("activity"), list):
                new_element["activity"] = " ".join(new_element["activity"])

            if not new_element.get("objects"):
                new_element["objects"] = ["Default_Object"]

            processed_list.append(new_element)

        return processed_list

    df_episodes["Episode"] = df_episodes["Episode"].apply(process_episode)

    df = df_episodes

    # Filter episodes by support range
    sup_min, sup_max = float(df["Support"].min()), float(df["Support"].max())
    support_range = st.slider(
        "Support range",
        min_value=sup_min,
        max_value=sup_max,
        value=(sup_min, sup_max),
    )
    filtered_df = df[df["Support"].between(support_range[0], support_range[1])]

    # # Scan which activities are mentioned in each episode
    activities = set()
    for episode_str in filtered_df["Episode"]:
        # Convert string back to list before iterating
        episode_list = episode_str
        for entry in episode_list:
            activities.update(entry["activity"])
    available_activities = sorted(activities)

    # # Scan which objects are mentioned in each episode
    objects = set()
    for episode_str in filtered_df["Episode"]:
        episode_list = episode_str
        for entry in episode_list:
            objects.update(entry["objects"])
        available_objects = sorted(objects)

    selected_activities = st.multiselect(
        "Episode must include all of these activities",
        options=available_activities,
    )

    selected_objects = st.multiselect(
        "Episode must include all of these objects",
        options=available_objects,
    )

    # filter the activities and objects
    if selected_activities:

        def episode_contains_selected_activities(episode_str):
            # Convert string back to list first
            episode_list = episode_str
            activities_in_episode = {
                activity for entry in episode_list for activity in entry["activity"]
            }
            return all(
                activity in activities_in_episode for activity in selected_activities
            )

        filtered_df = filtered_df[
            filtered_df["Episode"].apply(episode_contains_selected_activities)
        ]

    if selected_objects:

        def episode_contains_selected_objects(episode_str):
            episode_list = episode_str
            objects_in_episode = {
                objects for entry in episode_list for objects in entry["objects"]
            }
            return all(objects in objects_in_episode for objects in selected_activities)

        filtered_df = filtered_df[
            filtered_df["Episode"].apply(episode_contains_selected_objects)
        ]

    if filtered_df.empty:
        st.warning("No patterns match the current filters.")
        return

    # Sort the filtered DataFrame by support in descending order
    filtered_df = filtered_df.sort_values(by="Support", ascending=False)

    # Get patterns
    emma_patterns = filtered_df.to_dict(orient="records")

    # Display patterns in tiles with basic information
    for pattern in emma_patterns:
        pattern_id = pattern["PatternId"]
        support = pattern["Support"]
        with st.expander(f"Pattern ID: {pattern_id} (Support: {support})"):
            # When the expander is clicked, show detailed information
            st.write("### Episode Details")

            # Create network graph
            selected_pattern = pattern["Episode"]
            net = Network(height="800px", width="100%", directed=True)

            # Object-Colors
            all_objs = sorted(
                {obj for step in selected_pattern for obj in step["objects"]}
            )
            colors = ["#114b5f", "#698f3f", "#56203d", "#cc3f0c", "#2f2504"]
            color_map = {obj: colors[i % len(colors)] for i, obj in enumerate(all_objs)}

            # Create activity nodes
            activity_ids = []
            for i, step in enumerate(selected_pattern):
                act_id = f"a{i}"
                activity_ids.append((act_id, step["activity"], step["objects"]))

            # Create object map
            obj_event_map = defaultdict(list)
            for act_id, _, objects in activity_ids:
                for obj in objects:
                    obj_event_map[obj].append(act_id)

            # Calculate Layout
            positions = assign_node_positions(selected_pattern, obj_event_map)

            # Add activity nodes to graph
            for act_id, label, _ in activity_ids:
                x, y = positions[act_id]
                net.add_node(
                    act_id,
                    label=label,
                    shape="box",
                    color="#cccccc",
                    x=x,
                    y=y,
                    physics=False,
                    font={"color": "black"},
                )

            # Add object nodes and edges between activities
            transition_counter = defaultdict(int)
            for obj, events in obj_event_map.items():
                for src, tgt in zip(events, events[1:]):
                    transition_counter[(src, tgt)] += 1

            for obj, events in obj_event_map.items():
                start_id = f"obj_start_{obj}"
                end_id = f"obj_end_{obj}"
                color = color_map[obj]

                x_start, y_start = positions[start_id]
                x_end, y_end = positions[end_id]

                # Add object start and end nodes
                net.add_node(
                    start_id,
                    label=f"    Start: {obj}    ",
                    shape="ellipse",
                    color=color,
                    x=x_start,
                    y=y_start,
                    physics=False,
                    font={"color": "white"},
                    title=f"    {obj}    ",
                )
                net.add_node(
                    end_id,
                    label=f"    End: {obj}    ",
                    shape="ellipse",
                    color=color,
                    x=x_end,
                    y=y_end,
                    physics=False,
                    font={"color": "white"},
                    title=f"    {obj}    ",
                )

                # Add edges
                net.add_edge(start_id, events[0], color=color, width=2, title=obj)
                for src, tgt in zip(events, events[1:]):
                    roundness = 1.0 - (1.0 / transition_counter[(src, tgt)])
                    net.add_edge(
                        src,
                        tgt,
                        color=color,
                        width=2,
                        title=obj,
                        arrows="to",
                        physics=False,
                        smooth={
                            "enabled": True,
                            "type": "curvedCW",
                            "roundness": roundness,
                        },
                    )
                    transition_counter[(src, tgt)] -= 1
                net.add_edge(events[-1], end_id, color=color, width=2, title=obj)

            net.set_options(
                """
                {
                "physics": { "enabled": false },
                "interaction": {
                    "dragNodes": true,
                    "dragView": true,
                    "zoomView": true
                }
                }
                """
            )
            # Generate HTML Code for Network
            html = net.generate_html()

            # Run Fit on all nodes, to adapt Network to Window Size
            html += """
            <script type="text/javascript">
                network.once('afterDrawing', function() {
                    setTimeout(function() {
                        network.fit();
                    }, 50);
                });
            </script>
            """
            components.html(html, height=800)
    if st.button("Back"):
        if st.session_state.operation_mode == "uploadCSV":
            change_page("pattern_mining")

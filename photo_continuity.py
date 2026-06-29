PMEI_PHOTO_CONTINUITY_SCHEMA:
  schema_id: pmei-photo-continuity-v0.2
  owner: phil
  persona: Dave
  mode: local_read_only

  source:
    name: phil_pictures
    path: "C:\\Users\\Philip Garry\\Pictures"
    recursive: true
    enabled: true

  permissions:
    read_files: true
    write_files: false
    delete_files: false
    move_files: false
    upload_raw_images_by_default: false
    require_user_confirmation_before_upload: true

  include_extensions:
    - .jpg
    - .jpeg
    - .png
    - .bmp
    - .gif
    - .webp
    - .heic
    - .tif
    - .tiff

  exclude:
    filename_contains:
      - password
      - bank
      - passport
      - driving_license
      - private
      - payslip
      - tax

  scan_policy:
    scan_new_only: true
    hash_files: true
    hash_algorithm: sha256
    rescan_if_modified: true
    scan_interval_seconds: 60
    max_files_per_scan: 50
    maintain_scan_index: true
    skip_existing_hashes: true

  local_index:
    enabled: true
    index_file: "C:\\Users\\Philip Garry\\Pictures\\.pmei_photo_index.json"
    store:
      - file_path
      - file_hash
      - file_size
      - modified_time
      - scanned_time
      - project_match
      - confidence
      - status

  extraction:
    exif: true
    ocr: true
    visual_summary: true
    object_detection: true
    barcode_detection: true
    serial_number_detection: true
    timestamp_detection: true
    filename_analysis: true

  project_matching:
    enabled: true
    active_projects:
      - dell_5820_workstation
      - pmei
      - dave_runner
      - benchmarks
      - render_deployments

    keywords:
      dell_5820_workstation:
        - dell
        - precision
        - 5820
        - i9
        - 9980xe
        - cpu
        - ram
        - ssd
        - crystaldiskinfo
        - disk management
        - windows 11 pro

      pmei:
        - pmei
        - continuity
        - recursion
        - law
        - state
        - certification
        - cpv

      dave_runner:
        - dave runner
        - render
        - flask
        - postgres
        - server.py
        - deployment
        - gunicorn

      benchmarks:
        - br-001
        - br-002
        - br-002a
        - br-002b
        - br-002c
        - br-010
        - benchmark

  evidence_rules:
    raw_image_is_evidence: true
    screenshot_is_evidence: true
    extracted_text_is_observation: true
    visual_summary_is_observation: true
    inferred_state_requires_confirmation: true
    never_promote_inference_to_fact: true
    preserve_source_boundary: true

  candidate_generation:
    enabled: true
    confidence_threshold: 0.75
    create_candidate_for:
      - high_confidence_project_match
      - visible_error_message
      - benchmark_result
      - hardware_status
      - deployment_status
      - disk_health
      - system_configuration

  continuity_output:
    create_candidate_record: true
    auto_save_candidate: true
    auto_verify: false
    require_user_confirmation: true
    default_session_ref: image_evidence
    drift_score: 0.01
    seal: lawful

    template:
      human_title: "Image Evidence Candidate — {project_match}"
      human_summary: "{summary}"
      decision_made: "No decision made automatically."
      why_it_matters: "Image may provide supporting evidence for PMEi continuity."
      context_shard: |
        Source image: {file_path}
        File hash: {file_hash}
        Captured timestamp: {captured_timestamp}
        Scanned timestamp: {scanned_timestamp}
        OCR text: {ocr_text}
        Visual observations: {visual_observations}
        Project match: {project_match}
        Confidence: {confidence}
        Status: candidate evidence only until human confirmed.

      anchor_points:
        - image_evidence
        - photo_continuity
        - screenshot_continuity
        - "{project_match}"

  approval_workflow:
    required: true
    options:
      - approve_as_verified_continuity
      - save_as_evidence_only
      - ignore
      - request_manual_review
    default_action: save_as_evidence_only

  governance:
    evidence_before_explanation: true
    no_fabrication: true
    label_uncertainty: true
    human_final_authority: true
    no_private_folder_scan: true
    no_auto_delete: true
    no_auto_upload_raw_images: true

  dave_behavior:
    persona_id: dave-v1
    role: reflective_continuity_assistant
    reply_style: concise_practical
    default_response: >
      Treat image-derived data as evidence, not verified state,
      until Phil confirms it.

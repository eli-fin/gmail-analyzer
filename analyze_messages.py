import json
import download_messages


def read_output_to_json_array(output_file_path):
    with open(output_file_path, 'r', encoding='utf8') as f:
        return [json.loads(line) for line in f]


def get_header_value(msg, header_name):
    header_gen = (h for h in msg['payload']['headers']
                  if h['name'].lower() == header_name.lower())
    return next(header_gen, {'value': '<no-header>'})['value']


def print_messages_with_subject_count(messages, subject_substr):
    filtered = [m for m in messages if subject_substr in get_header_value(m,
                                                                          'subject').lower()]
    total_size_mb = sum([m['sizeEstimate'] for m in filtered])/1024/1024
    print(f"Found {len(filtered)} messages with subject containing '{subject_substr}' with a total size of {total_size_mb:.2f} MB")


def summarize_messages_by_header(messages, header_name, output_file_path, output_file_path_unique):
    '''Summarizes messages by a specific header, create a file with all values and a file with unique counts'''
    # (header_value, size)
    all_results = [(get_header_value(
        m, header_name).lower(), m['sizeEstimate']) for m in messages]
    all_results_unique = {}  # {header_value: (count, size)}
    for header_value, size in all_results:
        all_results_unique[header_value] = all_results_unique.get(
            header_value, (0, 0))
        all_results_unique[header_value] = (
            all_results_unique[header_value][0] + 1, all_results_unique[header_value][1] + size)

    with open(output_file_path, 'w', encoding='utf8') as f:
        for l in sorted([m[0] for m in all_results]):
            _ = f.write(f"{l}\n")

    with open(output_file_path_unique, 'w', encoding='utf8') as f:
        for m, (count, size) in sorted(all_results_unique.items(), key=lambda item: item[1], reverse=True):
            _ = f.write(
                f"count: {count}, size: {size/1024/1024:.2f} MB\t{m}\n")


def main():
    messages = read_output_to_json_array(download_messages.OUTPUT_FILE_PATH)

    print_messages_with_subject_count(messages, "")  # all messages
    print_messages_with_subject_count(messages, "login")

    summarize_messages_by_header(messages, 'subject',
                                 f'{download_messages.OUTPUT_FILE_FOLDER}/_all_subjects.txt',
                                 f'{download_messages.OUTPUT_FILE_FOLDER}/_all_subjects_unique.txt')
    summarize_messages_by_header(messages, 'from',
                                 f'{download_messages.OUTPUT_FILE_FOLDER}/_all_from.txt',
                                 f'{download_messages.OUTPUT_FILE_FOLDER}/_all_from_unique.txt')


if __name__ == "__main__":
    main()

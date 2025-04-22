from nidaq_module import init_task, read_task_data


def main () -> int:
    ini_path = "./API/NiDAQ.ini"
    task, samples_per_read, sample_rate, channel_names = init_task( ini_path )

    is_running = True
    while is_running:
        data = read_task_data( task ,samples_per_read )

    return 0


if __name__ == "__main__":
    main()

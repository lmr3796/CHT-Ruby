require 'open3'

class Worker
    def run_cmd(command, *args)
        # Should use wait_thr instead of $?; $? not working when using DRb
        stdin, stdout, stderr, wait_thr = Open3.popen3(command, *args)  #TODO: Possible with a chroot?
        result = {
            :stdout => stdout.readlines.join(''),
            :stderr => stderr.readlines.join(''),
            :exit_status => wait_thr.value
        }
        stdin.close
        stdout.close
        stderr.close
        return result
    end
end


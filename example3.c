#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include "io.h"

typedef uint16_t  u16;
typedef uint32_t  u32;
typedef uint64_t  u64;
typedef char      byte;
typedef ptrdiff_t size;
typedef size_t    usize;

#define countof(a)   (sizeof(a) / sizeof(*(a)))

struct data
{
    off_t base_off;
    off_t off;
    size base_len;
    size len;
    byte block[];
};

static io_handle inf;
static io_handle outf;
static size max_ring_entries;
static size file_sz;
static size block_sz;
static size read_off;
static size bytes_to_write;

static int get_file_size(io_os_handle fd, size *file_sz)
{
    struct stat st;

    if (fstat(fd, &st) != 0)
    {
        perror("fstat input file failed");
        return 1;
    }
    if (S_ISREG(st.st_mode))
    {
        *file_sz = st.st_size;
        return 0;
    }
    else if (S_ISBLK(st.st_mode))
    {
        u64 bytes;
        
        if (ioctl(fd, BLKGETSIZE64, &bytes) != 0)
        {
            perror("getting blocksize failed");
            return 1;
        }
        *file_sz = (size) bytes;
        return 0;
    }
    return 1;
}

bool read_callback(struct io_context *ioc, struct io_event *ev)
{
    struct data *data = ev->user;
    
    if (ev->evtype == IO_ERROR)
    {
        fprintf(stderr, "Read failed\n");
        return false;
    }
    if (ev->num < 0)
    {
        if (ev->num == -EAGAIN)
        {
            if (!io_read(ioc, data, inf, data->off,
                         data->block + (data->base_len - data->len),
                         (usize) data->len))
            {
                fprintf(stderr, "Read failed\n");
                return false;
            }
        }
        else
        {
            fprintf(stderr, "Read failed: %d, %s\n", abs(ev->num), strerror(abs(ev->num)));
            return false;
        }
    }
    else if (ev->num < data->len)
    {
        // Short read
        data->off += ev->num;
        data->len -= ev->num;
        if (!io_read(ioc, data, inf, data->off,
                     data->block + (data->base_len - data->len), (usize) data->len))
        {
            fprintf(stderr, "Read failed\n");
            return false;
        }
        
    }
    else
    {
        // Success!  Write out the data that was read.
        data->off = data->base_off;
        data->len = data->base_len;
        if (!io_write(ioc, data, outf, data->off, data->block, (usize) data->len))
        {
            fprintf(stderr, "Write failed\n");
            return false;
        }
    }
    return true;
}

bool write_callback(struct io_context *ioc, struct io_event *ev)
{
    struct data *data = ev->user;
    
    if (ev->evtype == IO_ERROR)
    {
        fprintf(stderr, "Write failed\n");
        return false;
    }
    if (ev->num < 0)
    {
        if (ev->num == -EAGAIN)
        {
            // Send again
            if (!io_write(ioc, data, inf, data->off,
                          data->block + (data->base_len - data->len), (usize) data->len))
            {
                fprintf(stderr, "Write failed\n");
                return false;
            }
        }
        else
        {
            fprintf(stderr, "Write failed: %s\n", strerror(abs(ev->num)));
            return false;
        }
    }
    else if (ev->num < data->len)
    {
        // Short write
        data->off += ev->num;
        data->len -= ev->num;
        bytes_to_write -= ev->num;
        if (!io_write(ioc, data, inf, data->off,
                      data->block + (data->base_len - data->len), (usize) data->len))
        {
            fprintf(stderr, "Write failed\n");
            return false;
        }
    }
    else
    {
        // Success!
        bytes_to_write -= ev->num;
        if (bytes_to_write == 0)
        {
            // Finished writing the entire file.
            return false;
        }
        else if (read_off < file_sz)
        {
            size read_sz = file_sz - read_off > block_sz ? block_sz : file_sz - read_off;
            data->base_off = read_off;
            data->off = read_off;
            data->base_len = read_sz;
            data->len = read_sz;
            if (!io_read(ioc, data, inf, read_off, data->block, (usize) read_sz))
            {
                fprintf(stderr, "Read failed\n");
                return false;
            }
            read_off += read_sz;
        }
    }
    return true;
}

static int copy_file(struct io_context *ioc, size file_sz)
{
    read_off = 0;
    bytes_to_write = file_sz;
    size ring_entries = 0;

    // Prime the pump with as many read requests as possible.
    while (read_off < file_sz && ring_entries < max_ring_entries)
    {
        size read_sz = file_sz - read_off > block_sz ? block_sz : file_sz - read_off;
        struct data *data = calloc(1, sizeof *data + (usize) block_sz);
        data->base_off = read_off;
        data->off = read_off;
        data->base_len = read_sz;
        data->len = read_sz;
        if (!io_read(ioc, data, inf, read_off, data->block, (u32) read_sz))
        {
            fprintf(stderr, "Read failed\n");
            return 1;
        }
        read_off += read_sz;
        ring_entries++;
    }

    struct io_event ev;
    io_wait(ioc, &ev);
    if (ev.evtype == IO_ERROR || bytes_to_write != 0)
    {
        return 1;
    }

    return 0;
}

int main(int argc, char **argv)
{
    struct io_resource res[2];
    struct io_operation *ops;
    struct io_context ioc;
    
    if (argc < 5)
    {
        fprintf(stderr, "Usage: %s <infile> <outfile> <ring entries> <block size>\n",
                argv[0]);
        return EXIT_FAILURE;
    }
    max_ring_entries = atoi(argv[3]);
    block_sz = atoi(argv[4]);
    if (max_ring_entries <= 0 || block_sz <= 0)
    {
        fprintf(stderr, "Invalid parameter\n");
        return EXIT_FAILURE;
    }
    ops = malloc(sizeof *ops * (usize) max_ring_entries);
        
    io_global_init();
    if (!io_init(&ioc, res, ops, countof(res), (u16) max_ring_entries))
    {
        return EXIT_FAILURE;
    }

    inf = io_open_file(&ioc, argv[1], IO_ACCESS_RD);
    if (inf == IO_INVALID)
    {
        fprintf(stderr, "Opening input file failed\n");
        return EXIT_FAILURE;
    }    
    outf = io_create_file(&ioc, argv[2], IO_CREATE_OVERWRITE);
    if (outf == IO_INVALID)
    {
        fprintf(stderr, "Opening output file failed\n");
        return EXIT_FAILURE;
    }
    io_set_callback(&ioc, inf, read_callback);
    io_set_callback(&ioc, outf, write_callback);
    
    if (get_file_size(res_from_handle(&ioc, inf)->os_handle, &file_sz) != 0)
    {
        return EXIT_FAILURE;
    }

    if (copy_file(&ioc, file_sz) != 0)
    {
        return EXIT_FAILURE;
    }

    io_close(&ioc, outf);
    io_close(&ioc, inf);
    io_free(&ioc);
    io_global_free();
    return EXIT_SUCCESS;
}

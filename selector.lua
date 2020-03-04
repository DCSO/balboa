function process(o)
    print("observation fields:")
    print(o:rcode())
    print(o:rdata())
    print(o:rrtype())
    print(o:rrname())
    print(o:sensor_id())

    print("tags:")
    local tags = o:tags()
    for i=1,#tags do
        print(" - " .. tags[i])
    end

    print("setting tag")
    o:add_tag("foo")

    print("new tags:")
    tags = o:tags()
    for i=1,#tags do
        print(" - " .. tags[i])
    end
end
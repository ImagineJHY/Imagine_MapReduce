cd proto

protoc --cpp_out=. *.proto

file_list=$(ls *.pb.cc)

for filename in $file_list; do
    filename_without_extension=$(basename $filename .pb.cc)
    sed -i "s/$filename_without_extension.pb.h/Imagine_MapReduce\/$filename_without_extension.pb.h/g" $filename
done

mv *.pb.h ../include/Imagine_MapReduce/
mv *.pb.cc ../src/
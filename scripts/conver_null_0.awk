BEGIN { FS="|"; OFS="|"}
{
    if ($6 == "")
        $6 = "0"
    if ($7 == "")
        $7 = "0"
    print
}


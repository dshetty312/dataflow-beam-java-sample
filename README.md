We still use the ExtractCountryCodeFn to split each input line into a KV pair with the country code as the key.
Instead of using TextIO.write(), we now use FileIO.writeDynamic(). This allows us to write to different files based on a key.
The by() method specifies how to get the key for each element (in our case, it's already the key of our KV pair).
The via() method specifies how to write each element. We're using TextIO.sink() to write the values as text.
The withNaming() method specifies how to name the output files. We're using the key (country code) in the filename.
We're still using withNumShards(1) to ensure one file per country code.
We've added imports for StringUtf8Coder and KvCoder.
We've created a separate PCollection<KV<String, String>> called kvPairs and explicitly set its coder using setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())). This tells Beam how to encode and decode the KV pairs.
We've simplified the by() and via() methods in the FileIO.writeDynamic() call to use method references (KV::getKey and KV::getValue) instead of lambda expressions.

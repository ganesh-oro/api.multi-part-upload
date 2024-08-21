function makeSlug(name: String) {
    //converting name to lower case then spaces replacing with '-' 
    //removing other special chars
    return name.trim().toLowerCase().replace(/[ \/\&]/g, '-') // replacing spaces with - 
      .replace(/[^\w-]+/g, '') // removing special chars
      .replace(/(\-)\1+/ig, (str, match) => { // removing duplicate consecutive '-'
        return match[0]
      })
}
  
export { makeSlug }
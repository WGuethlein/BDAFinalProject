import {useState} from "react"
import { Input, InputGroup, InputRightElement, Button } from '@chakra-ui/react'
import styles from './styles/Search.module.css'

const Search = (props) => {
    const [show, setShow] = useState(false)
    const handleClick = () => setShow(!show)
    
    return (
        <div className={styles.wrapper}>
            <InputGroup size='md'>
                <Input
                    pr='20rem'
                    placeholder='Search'
                />
                <InputRightElement width='5rem'>
                    <Button h='100%' w='100%'  onClick={handleClick}>
                    Search
                    </Button>
                </InputRightElement>
            </InputGroup>
        </div>
      )
}

export default Search;
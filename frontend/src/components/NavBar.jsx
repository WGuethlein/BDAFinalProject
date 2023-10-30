import styles from './styles/NavBar.module.css'
import Search from './Search'
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faHouse } from '@fortawesome/free-solid-svg-icons'

const NavBar = (props) => {
    // logo, home button, search bar
    
    return (
        <div className={styles.wrapper}>
            <img className={styles.amazonLogo} src='/img/amazon.png' alt="home"/>
            <Search className={styles.search}/>
            <FontAwesomeIcon icon={faHouse} className={styles.home} />

        </div>
    )
}

export default NavBar;